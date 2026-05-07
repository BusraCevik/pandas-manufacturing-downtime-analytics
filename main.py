import os
from src.data_processing.data_preparation import prepare_data
from src.data_processing.feature_engineering import run_feature_pipeline
from src.analysis.event_analysis import run_event_analysis_pipeline
from src.analysis.hourly_analysis import run_hourly_analysis_pipeline
from src.analysis.daily_analysis import run_daily_analysis_pipeline
from src.visualization.plots import generate_visualizations
from src.visualization.dashboard import build_manufacturing_dashboard


BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'outputs')
DOCS_DIR  = os.path.join(BASE_DIR, 'docs')

RAW_EXCEL_PATH  = os.path.join(DATA_DIR, 'raw', 'dataset.xlsx')
CLEANED_DIR     = os.path.join(DATA_DIR, 'cleaned')
FEATURED_DIR    = os.path.join(DATA_DIR, 'featured')

FIGURES_PATH        = os.path.join(OUTPUT_DIR, 'figures')
TABLES_PATH         = os.path.join(OUTPUT_DIR, 'tables')
DASHBOARD_HTML_PATH = os.path.join(DOCS_DIR, 'index.html')


def main():

    prepare_data(
        raw_excel_path=RAW_EXCEL_PATH,
        cleaned_dir=CLEANED_DIR
    )

    run_feature_pipeline(
        cleaned_dir=CLEANED_DIR,
        featured_dir=FEATURED_DIR,
    )

    run_event_analysis_pipeline(
        featured_dir=FEATURED_DIR,
        output_dir=TABLES_PATH,
    )

    run_hourly_analysis_pipeline(
        featured_dir=FEATURED_DIR,
        output_dir=TABLES_PATH,
    )

    run_daily_analysis_pipeline(
        featured_dir=FEATURED_DIR,
        output_dir=TABLES_PATH,
    )

    generate_visualizations(
        featured_dir=FEATURED_DIR,
        tables_dir=TABLES_PATH,
        fig_dir=FIGURES_PATH,
    )

    build_manufacturing_dashboard(
        featured_dir=FEATURED_DIR,
        tables_dir=TABLES_PATH,
        output_html_path=DASHBOARD_HTML_PATH,
    )


if __name__ == '__main__':
    main()
