import os
import click
from bench_tool import (
    configuration_reader, 
    preprocessing, 
    statistics,
    dq_measurement,
    benchmarking
)


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
        output_dir=output_dir,
        chunk_size=50000
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
        output_dir=output_dir,
        chunk_size=50000
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
        input_file=input_file,
        chunk_size=config.CHUNK_SIZE
    )
    temp_file = preprocessing.add_inaccuracy(
        input_file=temp_file,
        deviation=config.DAVIATION,
        outlier_factor=config.OUTLIER_FACTOR,
        outlier_percentage=config.OUTLIER_PERCENTAGE,
        chunk_size=config.CHUNK_SIZE
    )
    temp_file = preprocessing.add_missing_values(
        input_file=temp_file,
        missing_percentage=config.MISSING_PERCENTAGE,
        chunk_size=config.CHUNK_SIZE
    )
    output_file = preprocessing.add_time_of_availability(
        input_file=temp_file,
        output_dir=output_dir,
        validity_period=config.VOLATILITY,
        outdated_percentage=config.OUTDATED_PERCENTAGE,
        chunk_size=config.CHUNK_SIZE
    )
    click.echo(f"✅ Processed file saved as {output_file}")
    preprocessing.clean_temp_files(input_file)

# Register subcommands under preprocess
preprocess.add_command(split)
preprocess.add_command(extract)
preprocess.add_command(prepare)



@click.command()
@click.argument("input_file", type=click.Path(exists=True))
def show_stat(input_file):
    """Statistics commands for data analysis."""
    statistics.calculate_statistics(input_file, chunk_size=50000)

@click.group()
def data_quality():
    """Data Quality Measurement commands."""
    pass

@click.command()
@click.argument("data_file", type=click.Path(exists=True))
@click.option("--window_size", 
            "-w", 
            type=int, 
            default=10000, 
            help="Window size for data quality measurement. Default: 10000")
@click.option("--volatility", 
            "-v", 
            type=int, 
            default=2000, 
            help="Volatility parameter for data quality measurement. Default: 2000")
def show(data_file, window_size, volatility):
    """Measures and shows the data quality measurement results."""
    print(f"📊 Measuring Data Quality with window size: {window_size} and volatility: {volatility}...")
    dq_measurement.measure_dqs(
        file_path_real=data_file,
        window_size=window_size,
        volatility=volatility,
        SHOW=True
    )

@click.command()
@click.argument("data_file", type=click.Path(exists=True))
@click.argument("result_file", type=click.Path(exists=True))
@click.option("--window_size", 
            "-w", 
            type=int, 
            default=10000, 
            help="Window size for data quality measurement. Default: 10000")
@click.option("--volatility", 
            "-v", 
            type=int, 
            default=2000, 
            help="Volatility parameter for data quality measurement. Default: 2000")
def verify(data_file, result_file, window_size, volatility):
    """Verifies the results of the qulity measurement queries."""
    print(f"📊 Verifying Data Quality with window size: {window_size} and volatility: {volatility}...")
    result_df = dq_measurement.measure_dqs(
        file_path_real=data_file,
        window_size=window_size,
        volatility=volatility,
        SHOW=False
    )
    dq_measurement.compare_results(result_df, result_file)

data_quality.add_command(verify)
data_quality.add_command(show)


@click.group()
def benchmark():
    """Benchmarking commands for performance analysis."""
    pass

@click.command()
@click.argument("result_file", type=click.Path(exists=True))
def analyze(result_file):
    """Analyzes the latency and throughput of an file from Odysseus."""
    benchmarking.calculate_latency_throughput(result_file)

@click.command()
@click.argument("result_files", nargs=-1, type=click.Path(exists=True))
@click.option("--show_plot", 
                "-s",
              is_flag=True, 
              help="If provided, displays the comparison plot.")
def compare(result_files, show_plot):
    """Compares the latency and throughput of multiple files and plots the results."""
    if not result_files:
        click.echo("❌ No files provided. Please provide at least one file.")
        return

    click.echo(f"📂 Processing {len(result_files)} files...")
    
    benchmarking.compare_files(result_files, show=show_plot)

# @click.command()
# @click.argument("input_file", type=click.Path(exists=True))
# @click.option("--output_dir", 
#               "-o", 
#               required=False, 
#               help="Output directory for prepared file")
# @click.option("--config", 
#               "-c", 
#               type=click.Path(exists=True), 
#               help="Path to the configuration file. If not provided, default paramters will be used")
# def process_baseline(input_file, output_dir, config):
#     """Processes a CSV file by adding missing values and noise and expired data. """
#     config = configuration_reader.ConfigReader(config)
#     if not output_dir:
#         output_dir = os.getcwd()   
#     os.makedirs(output_dir, exist_ok=True)
#     benchmarking.process_baseline_file(
#         input_file=input_file,
#         output_dir=output_dir,
#         chunk_size=config.WINDOW_SIZE
#     )

benchmark.add_command(analyze)
benchmark.add_command(compare)
# benchmark.add_command(process_baseline)

# Add commands to the CLI group
cli.add_command(preprocess, "preprocess")
cli.add_command(show_stat, "show-stats")
cli.add_command(data_quality, "data-quality")
cli.add_command(benchmark, "benchmark")

if __name__ == "__main__":
    cli()
