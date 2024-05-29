# es-ch-sync

Run `make`
Syncs status records from the ES database to the CH database.
The program will run until it is stopped or until there are no more records to sync.
The following environment variables control the program's behavior:

- `START_TIME`: The time to start syncing from. If not set, the program will start syncing one month from the STOP_TIME.
- `STOP_TIME`: The time to stop syncing at. If not set, the program will stop syncing at the current time.
- `BATCH_SIZE`: The number of elastic records to sync at a time. If not set, the program will sync 1000 records at a time.
- `TOKEN_IDS`: Comma-separated list of vehicle token IDs to sync. If not set, the program will sync all vehicle token IDs currently in the ClickHouse database.
- `SIGNALS`: Comma-separated list of signals to sync. If not set, the program will sync all signals.

## License

[Apache 2.0](LICENSE)
