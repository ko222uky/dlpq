/* standard library */
use std::borrow::Cow;
use std::fs::File;
use std::time::Instant;
/* Client for parsing TDS wire protocol */ 
use tiberius::{
    AuthMethod, 
    Client, 
    Config, 
    Query, 
    QueryItem,
    ColumnType::*,
    ColumnData,
    FromSqlOwned // conversion trait
};

/* Asynchronous runtime */
// use async_std::net::TcpStream; // DEPRECATED
use tokio::net::TcpStream;
use tokio_util::compat::{
    TokioAsyncWriteCompatExt, Compat
}; // tokio has their own traits AsyncRead and AsyncWrite
use futures::TryStreamExt; // for the try_next() method for QueryStream<'a>

/* Dataframe library */
use polars::{prelude::*};

/* for handling datetime type conversion */
use chrono::{NaiveDate, NaiveDateTime};

/************************************************************
                    NOTES & CODE SNIPPETS
*************************************************************

The following was a wrong-headed approach, wherein I began making a custom enum to
handle types not known at compile time (so dynamic).

// #[derive(Debug)]
// pub enum TempValue {
//     TempNull,
//     TempInteger16(Option<i16>),
//     TempInteger32(Option<i32>),
//     TempInteger64(Option<i64>),
//     TempFloat32(Option<f32>),
//     TempFloat64(Option<f64>),
//     TempString(Option<std::string::String>),
//     TempBool(Option<bool>)
// }

// impl<'a> From<ColumnData<'a>> for TempValue {
//     fn from(data: ColumnData<'a>) -> Self {
//         match data {

//             ColumnData::I16(val) => TempValue::TempInteger16(val),
//             ColumnData::I32(val) => TempValue::TempInteger32(val),
//             ColumnData::I64(val) => TempValue::TempInteger64(val),

//             ColumnData::F32(val) => TempValue::TempFloat32(val),
//             ColumnData::F64(val) => TempValue::TempFloat64(val),
//             _ => TempValue::TempNull
//         }
//     }
// }



Also remember to use anyhow::Result<()>, with '?' for easy, general error propagation
 
*************************************************************/ 

/**********************************************************
 * 
 *        HELPER FUNCTIONS FOR DATA HANDLING / PARSING
 * 
***********************************************************/

// This may seem verbose, but it makes sense (for now) and can be
// improved later. I'll explain:
// I need to bridge the Tiberius ColumnData Enum variants to
// building a polars Series. I use a custom struct TiberiusColumn
// as an intermediate layer. This is due to rules on implementing external traits.
// Thus, I use this TiberiusColumn to wrap a Vec<ColumnData>, and then I can implement
// the From<&TiberiusColumn> and TryFrom<TiberiusColumn> traits for Series.


/// Data structure acting as a bridge betwen `Tiberius`' `ColumnData` and
/// the `polars::prelude::Series`
pub struct TiberiusColumn<'a> {
    pub name: String,
    pub data: Vec<ColumnData<'a>>
}

// These Trait implementations use pattern matching to map ColumnData vals to
// AnyValue, which can then be used to build our Polars Series with from_any_values.
// Note that the ColumnData is NOT the same as the Metadata column types, which reflects
// the TDS protocol data typing.

impl<'a> From<&TiberiusColumn<'_>> for Series {
    fn from(col: &TiberiusColumn<'_>) -> Self {

        // use AnyValues to get map ColumnData,
        // and we can then build Series from Vec<AnyValue>
        let any_values: Vec<AnyValue> = col.data
            .iter()
            .map(|data| match data {
                ColumnData::I32(Some(val)) => AnyValue::Int32(*val),
                ColumnData::I64(Some(val)) => AnyValue::Int64(*val),

                ColumnData::F32(Some(val)) => AnyValue::Float32(*val),
                ColumnData::F64(Some(val)) => AnyValue::Float64(*val),


                ColumnData::Bit(Some(val)) => AnyValue::Boolean(*val),
                ColumnData::String(Some(val)) => AnyValue::String(val.as_ref()),
   
                // Fallback for types you haven't implemented yet
                _ => AnyValue::Null, 
            }).collect();

            // build Series straight from Vec<AnyValue>
            Series::from_any_values(
                PlSmallStr::from_str(col.name.as_str()),
                &any_values,
                false
            ).expect("Failed to build Series from TiberiusColumn")        

    }
}

impl<'a> TryFrom<TiberiusColumn<'_>> for Series {
    type Error = PolarsError;

    fn try_from(col: TiberiusColumn<'_>) -> PolarsResult<Self> {
        let name = col.name.as_str();

        //  deduce the runtime column type.
        let first_item = match col.data.first() {
            Some(item) => item,
            None => return Ok(Series::new_empty(name.into(), &DataType::String)),
        };

        // Pattern match the Tiberius types and map to Polars Series
        let series = match first_item {

            // match arms pull the values out from the Enum
            ColumnData::I32(_) => {
                let vals: Vec<Option<i32>> = col.data.into_iter().map(|cd: ColumnData<'_>| match cd {
                    ColumnData::I32(val) => val,
                    _ => None,
                }).collect();
                Series::new(name.into(), vals)
            }
            ColumnData::I64(_) => {
                let vals: Vec<Option<i64>> = col.data.into_iter().map(|cd: ColumnData<'_>| match cd {
                    ColumnData::I64(val) => val,
                    _ => None,
                }).collect();
                Series::new(name.into(), vals)
            }

            ColumnData::F32(_) => {
                let vals: Vec<Option<f32>> = col.data.into_iter().map(|cd: ColumnData<'_>| match cd {
                    ColumnData::F32(val) => val,
                    _ => None,
                }).collect();
                Series::new(name.into(), vals)
            }

            ColumnData::F64(_) => {
                let vals: Vec<Option<f64>> = col.data.into_iter().map(|cd: ColumnData<'_>| match cd {
                    ColumnData::F64(val) => val,
                    _ => None,
                }).collect();
                Series::new(name.into(), vals)
            }

            ColumnData::Bit(_) => {
                let vals: Vec<Option<bool>> = col.data.into_iter().map(|cd: ColumnData<'_>| match cd {
                    ColumnData::Bit(val) => val,
                    _ => None,
                }).collect();
                Series::new(name.into(), vals)
            }
            ColumnData::String(_) => {
                // Tiberius strings use Cow<'a, str>; convert them to owned Option<String> for Polars
                let vals: Vec<Option<String>> = col.data.into_iter().map(|cd: ColumnData<'_>| match cd {
                    ColumnData::String(val) => extract_string(val), //val.map(|cow| cow.into_owned()),
                    _ => None,
                }).collect();
                Series::new(name.into(), vals)
            }
            // Add custom handlers for Date, Guid, or Binary variants if your DB relies on them
            _ => {
                return Err(PolarsError::ComputeError(
                    format!("Unsupported Tiberius conversion fallback for column: {}", name).into()
                ));
            }
        };

        Ok(series)
    }
}

/// Extracts an Option<String> from a cow (clone-on-write) smart pointer.
/// This is because `ColumnData::String` in `Tiberius` uses `Cow<'a, str>`
fn extract_string(cow_str: Option<Cow<'_, str>>) -> Option<String> {
    // But looks like a nightly compiler feat, since docs reference /nightly
    cow_str.as_ref().map(|s| s.to_string())
}

/// Maps the `Tiberius::ColumnData` to `String`.
/// For the simplest, headache-free approach, just map everything to String.
fn column_data_to_string(data: ColumnData<'static>) -> String {
    match data {
        ColumnData::String(opt) => opt.map(|s| s.into_owned()).unwrap_or_default(),

        ColumnData::U8(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),
        ColumnData::I16(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),
        ColumnData::I32(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),
        ColumnData::I64(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),

        ColumnData::F32(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),
        ColumnData::F64(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),

        ColumnData::Bit(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),
        ColumnData::Guid(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),

        // traitbounds not satisfied for date/datetimes, so the following doesn't work; 
        // found solution to use chrono
        
        // ColumnData::DateTime(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),  
        // ColumnData::DateTime2(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),
        // ColumnData::Date(opt) => opt.map(|v| v.to_string()).unwrap_or_default(),  

        ColumnData::DateTime(_) => {
            match NaiveDateTime::from_sql_owned(data) {
                Ok(Some(dt)) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                _ => String::new(), // empty string
            }
        },

        ColumnData::DateTime2(_) => {
            match NaiveDateTime::from_sql_owned(data) {
                Ok(Some(dt)) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                _ => String::new(), // empty string
            }
        },


        ColumnData::Date(_) => {
            match NaiveDate::from_sql_owned(data) {
                Ok(Some(dt)) => dt.format("%Y-%m-%d").to_string(),
                _ => String::new(), // empty string
            }
        }

        _ => String::new(), // Fallback safely to empty string for unsupported/complex types

    }
}


/**********************************************************
 * 
 *              HELPER FUNCTIONS FOR CONFIG / QUERYING
 * 
***********************************************************/

/// Returns a `Config` with `AuthMethod::Integrated`
pub fn get_config_integrated_auth(host: String, port: u16, database: String, trust: bool) -> Config {
    // Configuration options can be constructed with the builder method.
    let mut config: Config = Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::Integrated);
    if trust {config.trust_cert()};
    config

}

/// Builds a `Query` object from a String.
/// Does not bind any parameters.
pub fn get_select_query(query: String) -> Query<'static> {
    let query: Query<'_> = Query::new(query);
    query
}

/// Writes a `polars::frame::DataFrame` to a parquet file.
pub fn write_parquet(df: &mut DataFrame, parquet_file: String) -> Result<(), Box<dyn std::error::Error>> {
    // A function to wrap writing dataframe to parquet
    // we need to create target file prior to using ParquetWriter
    let file = File::create(parquet_file).map_err(
        |e| PolarsError::ComputeError(e.to_string().into()))?;

    let _ = ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None)) // or Snappy for more compatibility with older readers...
        .finish(&mut *df)?;

    Ok(())
}


/**********************************************************
 * 
 *              IMPORTANT DRIVING FUNCTION
 * 
***********************************************************/
/// Downloads a SQL query result to a target parquet file.
/// Requires a predefined `Query` and `Config`. Two main
/// process branches are available by `map2string`.
/// If `map2string` is `true`, then all values are mapped
/// to a `String`. `None` variants are represented as empty strings.
/// 
/// If `map2string` is `false`, then the `None` variant will map
/// to null in the `Polars` dataframe.
pub async fn dlpq(
    config: Config, 
    query: Query<'_>, 
    map2string: bool,
    parquet_file: String) -> anyhow::Result<()> {

    let start = Instant::now();

    // setup tcp stream
    let tcp: TcpStream = TcpStream::connect(config.get_addr()).await?;

    // Disables Nagle algorithm; buffering is handled
    // internally with a `Sink.`; Nagle's algorithm is
    // supposed to reduce # of small packets sent over the network.
    // But there are hiccups like ACK delays due to historical reasons:
    // https://en.wikipedia.org/wiki/Nagle%27s_algorithm
    tcp.set_nodelay(true)?;

    // This handles the TLS handshake, login and other details
    // specified by the SQL Server TDS wire protocol specs.
    // The token stream for actually data is the
    // COLMETADATA token (defines table schema, in a sense) 
    // followed by the ROW token stream, which contains the complete rows.
    // So TDS is implemented row-first.
    // Annotated here to make it explicit, since I swapped from the deprecated async-std
    let mut client: Client<Compat<TcpStream>> = Client::connect(config, tcp.compat_write()).await?;

    // Comment copied from docs:
    //      A response to a query is a stream of data, that must be
    //      polled to the end before querying again. Using streams allows
    //      fetching data in an asynchronous manner, if needed.
    //
    // Expect first the metadata, followed by the resulting rows
    let mut stream: tiberius::QueryStream<'_> = query.query(&mut client).await?;



    let mut colnames: Vec<String> = Vec::new(); 
    let mut numcols: usize = 0;
    // datatypes mapped from Tiberius -> Polars (and tiberius maps it from TDS -> Tiberius enums).
    // future work may use utilize this, but it's currently for visually checking dtypes from TDS wire protocol
    let mut dtypes: Vec<DataType> = Vec::new(); 

    



    /* Parses TDS data types. Originally intended to specify series dtypes */ 
    // Get metadata, which __should__ be the first item:
    // Read about QueryStream: https://docs.rs/tiberius/latest/tiberius/struct.QueryStream.html
    // NOTE to code-reviewers / learners: Syntax requires some control flow structure,
    // so start with 'if let' or 'while let'
    if let Some(item) = stream.try_next().await? {

        // We have a nested match, since we also get result for
        // item metadata, if it's the 0-index ResultMetadata
        match item {

            // We expect the first item to contain the column metadata,
            // according to TDS specifications.
            QueryItem::Metadata(ref meta) if meta.result_index() == 0 => {

                // for the metadata at index 0, we access that metadata and printout column types
                if let Some(metadata) =  item.into_metadata() {
                    let result = metadata;                
                    
                    // we can get the # of columns now, which we wouldn't have known beforehand
                    numcols = result.columns().len();
                    println!("Number of columns: {}", numcols);

                    // this preservers column order as viewed in the database
                    // for TDS types like Int<n>, n refers to number of bytes.
                    for col in result.columns().iter() {
                        
                        // you can pretty-print the entire metadata...
                        // println!("Full Result Metadata: {:#?}", result);
                        println!("Column Name: {:#?}, Column Type: {:#?}", col.name(), col.column_type());

                        colnames.push(col.name().into()); // grab names

                        if map2string {
                            dtypes.push(DataType::String); // catch-all type
                        } else {
                            match col.column_type() {


                                // this mapping may not be used, if I map everything to String for simplicity,
                                // or if I use the TryFrom / From trait implementations instead.
                                // Still, I'll pull this data and match it just in case.
                                Int1 => {dtypes.push(DataType::Int8);},
                                Int2 => {dtypes.push(DataType::Int16);},
                                Int4 => {dtypes.push(DataType::Int32);},
                                Int8 => {dtypes.push(DataType::Int64);},

                                Float4 => {dtypes.push(DataType::Float32);},
                                Float8 => {dtypes.push(DataType::Float64);},

                                Bit => {dtypes.push(DataType::Boolean);},
                                Guid => {dtypes.push(DataType::String);},

                                BigVarChar => {dtypes.push(DataType::String);},
                                NChar => {dtypes.push(DataType::String);},
                                NVarchar => {dtypes.push(DataType::String);},

                                // NOTE: We should try to convert TDS datetime to polars String (default behavior for now)
                                Daten => {dtypes.push(DataType::Date);},
                                Datetime => {dtypes.push(DataType::Datetime(TimeUnit::Microseconds, None));},
                                Datetime2 => {dtypes.push(DataType::Datetime(TimeUnit::Microseconds, None));},

                                _ => {}
                            }
                        } 
                        
                    };  

                    println!("\n")   
                                     
                }
            },

            // ... likewise, we shouldn't ever have a QueryItem::Row(_) in our first try_next().
            QueryItem::Row(_) => {
                panic!("Expected first item to be the column metadata.")
            },

            // ... Also, we should only have one query, so there shouldn't be a second col metadata,
            // with a meta.result_index() != 0
            QueryItem::Metadata(_) => {
                panic!("Expected first item to be the column metadata with meta.result_index() == 0")
            }
        }
    }
    
    /**********************************************************
     * 
     *              BUILDING COLUMN VECTORS
     * Here, we have two process branches, one where we map
     * everything to Strings, and another were we try to respect
     * the original dtype. Note that these branches should align
     * with the data type match patterns above.
     * 
     * This goes from QueryStream items -> Vec<Vec< whatever type >>
     * and then later we go Vec<Vec< whatever type >> -> Vec<TiberiusColumn>
     * -> Vec<Vec<Column>>
     * 
     * This seems a bit verbose, but the issue is due to two
     * process branches that I've created, namely, that:
     *      (1) If we convert everything to String, then we can know
     *          the data type at compile time.
     * 
     *      (2) Otherwise, we cannot know the data type at compile time.
     *          So, we used the custom ColumnData Enum provided by
     *          Tiberius, and we build that into the TiberiusColumn,
     *          which is my custom data struct; and for TiberiusColumn,
     *          I implemented a try_from() to map it to a PolarsResult<Series>
     * 
     * With the Series object, I can easily create the columns
     * for my dataframe.
    ***********************************************************/

    // containers that we will use to build our Polars series.
    // Base init is Vec<Vec<String>>, but this can be shadowed later.
    let mut data: Vec<Vec<String>> = vec![Vec::new(); numcols];

    // for the dynamic case, I use the custom struct and make 
    let mut tc_data: Vec<TiberiusColumn> = vec![];

    // Now, we build column vectors
    if map2string{

        // in this case, we know that our data can be a Vec<Vec<String>>

        while let Some(item) = stream.try_next().await? {
            match item {

                // I feel like this is far from idiomatic rust, but it makes sense to me now.
                QueryItem::Row(row) => {
                    for (i, (_col, col_data)) in row.cells().enumerate() {
                        let val_str = column_data_to_string(col_data.clone());
                        data[i].push(val_str)
                    }
                },

                // placeholders for the rest of patterns
                _ => {}
            }
        }

        // PRINT LOOP FOR DEBUGGING
        // // check everything that it's what we'd expect.
        // for (i, dcol) in data.iter().enumerate() {
        //     println!("Data type: {}", dtypes[i]);
        //     println!("{:#?}", dcol);
        // }


    } else {

        // for cases where we don't know at compile time, shadow the data variable
        let mut data: Vec<Vec<ColumnData>> = vec![Vec::new(); numcols]; // shadow with different typing

        while let Some(item) = stream.try_next().await? {
            match item {

                // I feel like so far, what I've been doing is far from idiomatic rust, but it makes sense to me now.
                QueryItem::Row(row) => {
                    for (i, col_data) in row.into_iter().enumerate() {
                        let val = col_data.clone();

                        
                        data[i].push(val);
                    }
                },

                // placeholders for the rest of patterns
                _ => {}
            }
        }

        // building our TiberiusColumns

        for (i, col) in data.iter().enumerate() {
            let tiberius_col = TiberiusColumn{
                name: colnames[i].clone(),
                data: col.to_vec()
            };

            tc_data.push(tiberius_col);
        }
        
        // check everything that it's what we'd expect.
        // for (i, dcol) in data.iter().enumerate() {
        //     println!("Data type: {}", dtypes[i]);
        //     println!("{:#?}", dcol);
        // }   
    }


    /**********************************************************
     * 
     *              BUILDING SERIES & WRITING DATAFRAME
     * 
    ***********************************************************/
    
    let mut columns: Vec<Column> = vec![];
    let num_rows: usize;

    if map2string {
        // since everything is a string, making the series is easy.
        // Building from vectors of Strings
        for (i, col) in data.iter().enumerate() {
            let s = Series::new(
                colnames[i].clone().into(),
                &col
            );
            columns.push(Column::from(s));
        };

        // construct dataframe:
        // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#constructing-from-a-veccolumn
        let mut df = DataFrame::new_infer_height(columns)?;
        (num_rows, _) = df.shape();
        // we need to create target file prior to using ParquetWriter
        let _ = write_parquet(&mut df, parquet_file); // we don't really need the return in this case. ignore it.



    } else {

        // building using TiberiusColumn custom data struct,
        // and our implemented traits
        for col in tc_data.iter() {
            let s = Series::try_from(col)?;     
            columns.push(Column::from(s));
        }

        // construct dataframe:
        let mut df = DataFrame::new_infer_height(columns)?;
        (num_rows, _) = df.shape();
        let _ = write_parquet(&mut df, parquet_file);
    }

    println!("Downloaded {} rows in time {:?}", num_rows, start.elapsed());

    Ok(())
}




/*******************************************************
 
                        TEST CODE 

********************************************************/
// Can start populating this with test cases.

/// Boilerplate code for an example function for a unit test.
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

/// Test boilerplate code
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    

}
