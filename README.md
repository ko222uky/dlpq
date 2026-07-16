# dlpq 🥒

Data layer for downloading a SQL Server table (or query result) to a Parquet file.

`dlpq` connects to SQL Server over TDS 7.3 using [tiberius](https://docs.rs/tiberius), runs a query, and writes the results to a Parquet file using [polars](https://docs.rs/polars).

Additional functions may be added around the `polars` dataframes in the future.

## Usage

```rust
use dlpq::{dlpq, get_config_integrated_auth, get_select_query};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = get_config_integrated_auth(
        "localhost".to_string(),
        1433,
        "MyDatabase".to_string(),
        true, // trust server certificate
    );

    let query = get_select_query("SELECT * FROM MyFavoriteTable".to_string());

    dlpq(config, query, /* map2string = */ false, "output.parquet".to_string()).await?;

    Ok(())
}
```

### `map2string`

The `dlpq` function has two processing modes, controlled by the `map2string` flag:

- `true` — every column value is converted to a `String`. `NULL` values become empty strings.
- `false` — column types are inferred from the TDS column metadata and mapped to native Polars types. `NULL` values are preserved as nulls.

## Authentication

Currently only Windows Integrated Authentication is supported via `get_config_integrated_auth`.

## Status

Early / in-progress project. Type coverage for SQL Server column types (dates, GUIDs, binary, etc.) is still being filled in, but current covered types should be sufficient for most tables.

## License

Licensed under either of

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
