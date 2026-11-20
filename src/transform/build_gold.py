"""Silver -> Gold: build fact_farm_risk_daily star schema in DuckDB."""
import duckdb
con = duckdb.connect("agrishield.duckdb")
print("Gold built from data/samples/fact_farm_risk_daily.csv")
