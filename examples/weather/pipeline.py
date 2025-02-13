from datetime import datetime as dt
from pathlib import Path
from random import uniform

import polars as pl
from pydantic import BaseModel


class Config(BaseModel):
    input_dir: Path = Path("input")
    output_dir: Path = Path("output")
    stations: list[str] = ["a", "b"]
    start: dt = dt(2023, 1, 1)
    end: dt = dt(2023, 12, 31)


class AnalysisResult(BaseModel):
    avg_temperature: float
    avg_humidity: float
    max_temperature: float
    min_temperature: float


def generate_weather_data(config: Config) -> None:
    config.input_dir.mkdir(exist_ok=True)
    for station in config.stations:
        data = [
            {
                "date": date.strftime("%Y-%m-%d"),
                "temperature": round(uniform(-10, 35), 1),
                "humidity": round(uniform(20, 100), 1),
                "pressure": round(uniform(950, 1050), 1),
                "wind_speed": round(uniform(0, 100), 1),
            }
            for date in pl.date_range(start=config.start, end=config.end, eager=True)
        ]
        df = pl.DataFrame(data)
        df.write_csv(config.input_dir / f"raw_{station}.csv")


def extract_station_data(config: Config, station: str) -> None:
    config.output_dir.mkdir(exist_ok=True)
    input_file = config.input_dir / f"raw_{station}.csv"
    output_file = config.output_dir / f"processed_{station}.csv"
    df = pl.read_csv(input_file)
    df.select(["date", "temperature", "humidity"]).write_csv(output_file)


def merge_station_data(config: Config) -> None:
    dfs = [
        pl.read_csv(config.output_dir / f"processed_{station}.csv").with_columns(
            pl.lit(station).alias("station")
        )
        for station in config.stations
    ]
    pl.concat(dfs).write_csv(config.output_dir / "merged_data.csv")


def analyze_data(config: Config) -> AnalysisResult:
    df = pl.read_csv(config.output_dir / "merged_data.csv")
    analysis = AnalysisResult(
        avg_temperature=df["temperature"].mean(),
        avg_humidity=df["humidity"].mean(),
        max_temperature=df["temperature"].max(),
        min_temperature=df["temperature"].min(),
    )
    pl.DataFrame([analysis.dict()]).write_json(
        config.output_dir / "analysis_results.json"
    )
    return analysis


def generate_summary(config: Config, analysis: AnalysisResult) -> None:
    summary_lines = [
        "Climate Data Summary",
        *(
            f"{field.description}: {getattr(analysis, field_name):.2f}"
            for field_name, field in AnalysisResult.model_fields.items()
        ),
    ]
    summary = "\n".join(summary_lines)
    (config.output_dir / "summary.txt").write_text(summary)


def run_pipeline(config: Config = Config()) -> None:
    generate_weather_data(config)
    [extract_station_data(config, station) for station in config.stations]
    merge_station_data(config)
    analysis = analyze_data(config)
    generate_summary(config, analysis)


if __name__ == "__main__":
    run_pipeline()
