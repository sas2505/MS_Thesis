import os
import click
from bench_tool import configuration_reader, preprocessing


LOGO = (
    " ___                 _           _____            _ \n"
    "| _ ) ___  _ _   __ | |_        |_   _| ___  ___ | |\n"
    "| _ \/ -_)| ' \ / _||   \         | |  / _ \/ _ \| |\n"
    "|___/\___||_||_|\__||_||_|        |_|  \___/\___/|_|\n"

)

@click.group()
def cli():
    """Bench-Tool: A CLI for data preprocessing, statistics, and benchmarking"""
    click.echo(LOGO)

@click.group()
def preprocess():
    """Preprocessing commands for data preparation."""
    pass

@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--output_dir", "-o", required=False, help="Output directory for split files")
def split(input_file, output_dir):
    """Splits a large CSV file into smaller files based on sensor_id."""
    if not output_dir:
        output_dir = os.getcwd()
        
    os.makedirs(output_dir, exist_ok=True)
    preprocessing.split_sensors_by_file(
        input_file=input_file, 
        output_dir=output_dir
    )

@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--days", "-d", type=int, default=7, help="Number of days to extract. Default: 7")
@click.option("--output_dir", "-o", required=False, help="Output directory for extracted file")
def extract(input_file, days, output_dir):
    """Extracts the first N days of data from a CSV file."""
    if not output_dir:
        output_dir = os.getcwd()   
    os.makedirs(output_dir, exist_ok=True)
    preprocessing.extract_first_no_of_days(
        sensor_file=input_file, 
        no_of_days=days, 
        output_dir=output_dir
    )
    

@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--output_dir", 
              "-o", 
              required=False, 
              help="Output directory for prepared file")
@click.option("--config", 
              "-c", 
              type=click.Path(exists=True), 
              help="Path to the configuration file. If not provided, default paramters will be used")
def prepare(input_file, output_dir, config):
    """Processes a CSV file by adding missing values and noise and expired data. """
    config = configuration_reader.ConfigReader(config)
    if not output_dir:
        output_dir = os.getcwd()   
    os.makedirs(output_dir, exist_ok=True)
    click.echo(f"🔄 Processing {input_file}...")
    temp_file = preprocessing.convert_datetime_to_timestamp(
        input_file=input_file
    )
    temp_file = preprocessing.add_inaccuracy(
        input_file=temp_file,
        deviation=config.DAVIATION,
        outlier_factor=config.OUTLIER_FACTOR,
        outlier_percentage=config.OUTLIER_PERCENTAGE
    )
    temp_file = preprocessing.add_missing_values(
        input_file=temp_file,
        missing_percentage=config.MISSING_PERCENTAGE
    )
    output_file = preprocessing.add_time_of_availability(
        input_file=temp_file,
        output_dir=output_dir,
        validity_period=config.VOLATILITY,
        outdated_percentage=config.OUTDATED_PERCENTAGE
    )
    click.echo(f"✅ Processed file saved as {output_file}")
    preprocessing.clean_temp_files(input_file)

# Register subcommands under preprocess
preprocess.add_command(split)
preprocess.add_command(extract)
preprocess.add_command(prepare)

# Add commands to the CLI group
cli.add_command(preprocess, "preprocess")
# cli.add_command(stats_cmd, "stats")
# cli.add_command(benchmark_cmd, "benchmark")

if __name__ == "__main__":
    cli()
