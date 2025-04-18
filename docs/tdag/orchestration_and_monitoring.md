# Orchestration & Monitoring

Once your time series pipelines are built, TDAG offers multiple modes for executing and monitoring their updates efficiently. These modes support local development, debugging, and scalable production deployments.

## Execution Modes

1. **Local Mode** We can run our pipeline locally using local parquet files without interacting with the remote database. This mode is ideal for fast prototyping or parameter sweeps (e.g., hyperparameter tuning). It is faster then the other modes as it does not perform costly database writes.
2. **Debug Mode**: We run our pipelines for one-loop as a single process, persisting and reading from our remote database. This is helpful for debugging and development before moving to production.
3. **Live Mode**: We run our pipelines as a separate distributed process via a Ray cluster. This mode is designed for production use.

## Running Time Series in Local Mode

For quick local development and testing of a new time series we can use the local data lake mode to run the time serie using
```
time_series = CryptoPortfolioTimeSerie()
result = time_series.run_local_update()
```

A classic use-case is to see how a strategy performs with different parameters  by running it in a loop. Here we have a Long Short portfolio and we want to observe the hyperspace of portfolios generated by several combinations of parameters. In this case, we don’t want to persist any iteration in the database; perhaps we just want to see at which point the interaction of the regularization parameters starts to decrease, for example, or at which point our regression starts to stabilize. For these scenarios, we can run our pipelines in **Local Mode** mode.

Let’s look at a code example to understand it better.

```python
total_return = []
for rolling_window in range(60, 30 * 24, 20):
    for lasso_alpha in [1, 1e-2, 1e-3, 1e-4, 1e-5]:
        long_short_portfolio = LongShortPortfolio(
           ticker_long="XLF", 
           ticker_short="XLE",
           long_rolling_windows=[long_rollling_window],
           short_rolling_windows=[100, 200], 
           lasso_alpha=1e-2
        )
        portfolio_df = long_short_portfolio.run_local_update()
        total_return.append(long_short_portfolio["portfolio"].iloc[-1] - 1)

```

If we run in Local Mode, each of the TimeSeries will be dumped once from the database into a Data Lake as a Parquet file. The Data Lake will be configured in a folder structure of the following form:

``` 
DataLakeName/
├── DateRange/
│   ├── TimeSeriesHash1/parquet_partitions
│   ├── TimeSeriesHash2/parquet_partitions
│   ├── TimeSeriesHash3/parquet_partitions
│   └── ...
└── ...
```

## Running Time Series in Live/Debug mode
When we want to move our time series to production, we can execute backend system so it can be distributed and the data stored in the shared database for reusability. 
This is done using the .run() method. 
```
time_series = CryptoPortfolioTimeSerie()
time_series.run(debug_mode=False)
```

We can use additional parameters to specify how the timeseries should run.

- ```debug_mode```: A boolean Setting this to **True** runs the Pipeline in Debug Mode, otherwise in Live Mode.
- ```update_tree```: A boolean variable whether to update all the dependencies of the time series or only the called time series. This is helpful if this time series has many dependencies and we are only interested in the final time serie.
- ```update_only_tree```: A boolean variable whether to update only the dependencies of the time series.
- ```remote_scheduler```: An optional custom scheduler to run the time series. If no remote_scheduler is provided, a default scheduler is created automatically.
- ```force_update```: A scheduler manages at which times to run the time series. This boolean variable is used to ignore the scheduler.

For example, to run this time series immediately in debug mode and only update the called time series, we can use:
```
time_series = CryptoPortfolioTimeSerie()
time_series.run(debug_mode=True, update_tree=False, force_update=True)
```
