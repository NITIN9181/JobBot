import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from modules.exporter import export_to_csv, display_terminal_summary, export_to_google_sheets

# For salary formatting test, we need to import it from notifier though exporter also uses its own logic sometimes?
# Actually exporter.py has its own formatting in export_to_google_sheets (lines 167-168)
# but notifier.py has the dedicated format_salary function.
from modules.notifier import format_salary

def test_export_to_csv(sample_jobs_df, tmp_path):
    """Test that CSV export creates a file and has expected structure."""
    output_dir = tmp_path / "output"
    
    filepath = export_to_csv(sample_jobs_df, output_dir=str(output_dir))
    
    assert os.path.exists(filepath)
    df_loaded = pd.read_csv(filepath)
    assert len(df_loaded) == len(sample_jobs_df)
    assert "title" in df_loaded.columns
    assert "company" in df_loaded.columns

def test_display_terminal_summary_no_crash(sample_jobs_df):
    """Ensure display function handles empty and populated DataFrames gracefully."""
    # Test with data
    display_terminal_summary(sample_jobs_df)
    
    # Test with empty
    display_terminal_summary(pd.DataFrame())
    # No assertion needed, just verifying it doesn't raise Exception

def test_format_salary():
    """Test salary formatting utility."""
    assert format_salary(80000, 120000) == "$80K - $120K USD"
    assert format_salary(80000, None) == "$80K+ USD"
    assert format_salary(None, 150000) == "Up to $150K USD"
    assert format_salary(None, None) == "Not listed"
    assert format_salary(50000, 50000) == "$50K USD"

@patch("modules.exporter.setup_google_sheets")
def test_export_to_google_sheets_mock(mock_setup, sample_jobs_df, sample_config):
    """Test Google Sheets export with mocking gspread."""
    mock_spreadsheet = MagicMock()
    mock_worksheet = MagicMock()
    mock_setup.return_value = (mock_spreadsheet, mock_worksheet)
    
    # Mock col_values to return empty list (no duplicates)
    mock_worksheet.col_values.return_value = []
    
    status = export_to_google_sheets(sample_jobs_df, sample_config)
    
    assert status["success"] is True
    assert mock_worksheet.append_rows.called
