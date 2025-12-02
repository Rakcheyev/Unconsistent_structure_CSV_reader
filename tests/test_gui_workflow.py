"""
Automated GUI workflow test for USCSV DearPyGui UI
"""
import unittest
import os
from src.ui.workflow_backend import run_batch_workflow

class TestUSCSVGuiWorkflow(unittest.TestCase):
    def setUp(self):
        self.input_folder = "tests/data/input"
        self.output_folder = "tests/data/output"
        self.output_format = "CSV"
        self.memory_cap = 128
        self.chunk_size = 5000
        os.makedirs(self.input_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)
        # Create a dummy CSV file for testing
        with open(os.path.join(self.input_folder, "test.csv"), "w") as f:
            f.write("col1,col2\n1,2\n3,4\n")

    def tearDown(self):
        # Clean up test files
        for root, dirs, files in os.walk(self.input_folder):
            for file in files:
                os.remove(os.path.join(root, file))
        for root, dirs, files in os.walk(self.output_folder):
            for file in files:
                os.remove(os.path.join(root, file))

    def test_batch_workflow(self):
        # Run the backend workflow
        run_batch_workflow(
            self.input_folder,
            self.output_folder,
            self.output_format,
            self.memory_cap,
            self.chunk_size
        )
        # Check that output mapping file exists
        mapping_path = os.path.join(self.output_folder, "mapping.json")
        self.assertTrue(os.path.exists(mapping_path))
        # Check that output folder is not empty
        self.assertTrue(len(os.listdir(self.output_folder)) > 0)

if __name__ == "__main__":
    unittest.main()
