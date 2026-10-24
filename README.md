# Tick Lab price library

1-minute bid and ask prices used by the Tick Lab strategy tester's **Broker CFD feed**.

- `m1/<SYMBOL>/<YYYY-MM>.tlm1.gz`: one month of one symbol. The format is described at the top of `build.py`.
- `manifest.json`: which months exist for each symbol.

The prices come from Dukascopy's public historical data feed. They're rebuilt by the
**Build price library** GitHub Action, which runs on the 3rd of each month.

Past prices only. Nothing here is financial advice.
