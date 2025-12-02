"""
DearPyGui UI for Unconsistent_structure_CSV_reader
Allows user to select CSV, set SQLite DB path, and run workflow steps.
"""
import dearpygui.dearpygui as dpg
import os
from workflow_backend import run_batch_workflow

def run_workflow(input_folder, output_folder, output_format, memory_cap, chunk_size, progress_bar, log_window):
    try:
        dpg.set_value(log_window, "Starting workflow...\n")
        dpg.set_value(progress_bar, 0.0)
        run_batch_workflow(input_folder, output_folder, output_format, memory_cap, chunk_size)
        dpg.set_value(progress_bar, 1.0)
        dpg.set_value(log_window, dpg.get_value(log_window) + "Workflow complete!\n")
        # Show summary dialog
        import os
        output_files = os.listdir(output_folder) if os.path.exists(output_folder) else []
        with dpg.window(label="Summary", modal=True, no_close=False, width=400, height=220):
            dpg.add_text(f"Processing complete!")
            dpg.add_text(f"Output folder: {output_folder}")
            dpg.add_text(f"Files generated: {len(output_files)}")
            if output_files:
                dpg.add_text("Output files:")
                for fname in output_files:
                    dpg.add_text(f"- {fname}")
            dpg.add_button(label="Close", callback=lambda: dpg.delete_item(dpg.last_item()))
    except Exception as e:
        dpg.set_value(log_window, dpg.get_value(log_window) + f"Error: {e}\n")
        dpg.set_value(progress_bar, 0.0)
        with dpg.window(label="Error", modal=True, no_close=False, width=400, height=120):
            dpg.add_text(f"An error occurred:\n{e}")
            dpg.add_button(label="Close", callback=lambda: dpg.delete_item(dpg.last_item()))

def main():
    dpg.create_context()
    dpg.create_viewport(title='USCSV Workflow UI', width=600, height=400)

    # Localization-ready text
    TEXT = {
        "input_folder": "Select input folder (CSV files):",
        "output_folder": "Select output folder (results):",
        "output_format": "Select output format:",
        "memory_cap": "Set memory cap (MB):",
        "chunk_size": "Set chunk size (rows):",
        "run": "Run workflow:"
    }

    with dpg.window(label="USCSV Workflow", width=580, height=420):
        dpg.add_text(TEXT["input_folder"])
        input_folder = dpg.add_input_text(label="Input Folder", width=400, hint="Folder containing CSV/TSV files.")
        dpg.add_button(label="Browse Input Folder", callback=lambda: dpg.set_value(input_folder, dpg.select_directory()),
                       tip="Open a dialog to select the input folder.")

        dpg.add_text(TEXT["output_folder"])
        output_folder = dpg.add_input_text(label="Output Folder", width=400, hint="Folder to save processed results.")
        dpg.add_button(label="Browse Output Folder", callback=lambda: dpg.set_value(output_folder, dpg.select_directory()),
                       tip="Open a dialog to select the output folder.")

        dpg.add_text(TEXT["output_format"])
        output_format = dpg.add_combo(items=["CSV", "JSON"], default_value="CSV", width=200, tip="Choose the format for output files.")

        dpg.add_text(TEXT["memory_cap"])
        memory_cap = dpg.add_slider_int(label="Memory Cap (MB)", default_value=256, min_value=64, max_value=4096, width=200,
                                       tip="Maximum memory usage for processing (MB).")

        dpg.add_text(TEXT["chunk_size"])
        chunk_size = dpg.add_slider_int(label="Chunk Size (rows)", default_value=10000, min_value=1000, max_value=100000, width=200,
                                       tip="Number of rows per processing chunk.")

        dpg.add_separator()
        dpg.add_text(TEXT["run"])
        progress_bar = dpg.add_progress_bar(label="Progress", default_value=0.0, width=400, tip="Shows workflow progress.")
        log_window = dpg.add_input_text(label="Log", multiline=True, readonly=True, width=400, height=100, default_value="",
                                       tip="Displays workflow logs and messages.")
        dpg.add_button(label="Run", callback=lambda: run_workflow(
            dpg.get_value(input_folder),
            dpg.get_value(output_folder),
            dpg.get_value(output_format),
            dpg.get_value(memory_cap),
            dpg.get_value(chunk_size),
            progress_bar,
            log_window
        ), tip="Start processing with selected options.")

    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.start_dearpygui()
    dpg.destroy_context()

if __name__ == "__main__":
    main()
