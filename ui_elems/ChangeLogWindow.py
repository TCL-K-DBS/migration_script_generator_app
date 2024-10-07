# change_log_window.py

import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog,QMessageBox
from PyQt5.QtCore import Qt
from logics.ChangeLogComparator import LiquibaseChangelogComparer

class ChangeLogWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.current_xml = None  # To store the current XML file path
        self.previous_xml = None  # To store the previous XML file path

        self.initUI()

    def initUI(self):
        # Create a main vertical layout
        vbox = QVBoxLayout()

        # Create horizontal layouts for each button and corresponding label
        hbox_current = QVBoxLayout()
        hbox_previous = QVBoxLayout()

        # Create buttons
        self.current_btn = QPushButton("Your current/latest change log")
        self.previous_btn = QPushButton("Your previous change log")

        # Create labels to display file names under each button
        self.current_label = QLabel("No file selected")
        self.previous_label = QLabel("No file selected")

        # Connect buttons to the file dialog functions
        self.current_btn.clicked.connect(self.select_current_xml)
        self.previous_btn.clicked.connect(self.select_previous_xml)

        # Add the current button and label to the first horizontal layout
        hbox_current.addWidget(self.current_btn)
        hbox_current.addWidget(self.current_label)

        # Add the previous button and label to the second horizontal layout
        hbox_previous.addWidget(self.previous_btn)
        hbox_previous.addWidget(self.previous_label)

        # Create a horizontal layout to put both buttons side by side
        hbox_main = QHBoxLayout()
        hbox_main.addLayout(hbox_current)
        hbox_main.addLayout(hbox_previous)

        # Add the horizontal layout to the main vertical layout
        vbox.addLayout(hbox_main)

        # Create a "Generate Migration Script" button and add it between the two buttons
        self.generate_btn = QPushButton("Generate Migration Script")
        self.generate_btn.setEnabled(False)  # Initially disabled until files are selected
        self.generate_btn.clicked.connect(self.generate_migration_script)

        # Add the generate button below the file names, centered between both
        vbox.addWidget(self.generate_btn, alignment=Qt.AlignCenter)

        self.setLayout(vbox)
        self.setWindowTitle("Change Log Selector")
        self.show()

    def select_current_xml(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Select Current Change Log", "", "XML files (*.xml)")
        if file_name:
            self.current_xml = file_name  # Store the selected file path
            self.current_label.setText(
                f"Selected: {file_name.split('/')[-1]}")  # Display the file name under the button
            self.check_enable_generate_btn()  # Check if both files are selected to enable the migration button

    def select_previous_xml(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Select Previous Change Log", "", "XML files (*.xml)")
        if file_name:
            self.previous_xml = file_name  # Store the selected file path
            self.previous_label.setText(
                f"Selected: {file_name.split('/')[-1]}")  # Display the file name under the button
            self.check_enable_generate_btn()  # Check if both files are selected to enable the migration button

    def check_enable_generate_btn(self):
        """Enable the 'Generate Migration Script' button only if both files are selected."""
        if self.current_xml and self.previous_xml:
            self.generate_btn.setEnabled(True)  # Enable the button when both files are selected

    def generate_migration_script(self):
        """Handle the logic for generating the migration script."""
        # Placeholder logic to indicate this function is called
        print(f"Generating migration script from:\nCurrent: {self.current_xml}\nPrevious: {self.previous_xml}")

        previous_changelog_path = self.previous_xml  # Replace with the actual path to the previous XML
        current_changelog_path = self.current_xml  # Replace with the actual path to the current XML

        try:
            comparator = LiquibaseChangelogComparer(previous_changelog_path, current_changelog_path)
            new_changelog = comparator.compare_and_generate()

            # Open a file dialog for the user to select the export location
            options = QFileDialog.Options()
            file_path, _ = QFileDialog.getSaveFileName(self, "Save Migration Script", "",
                                                       "XML Files (*.xml);;All Files (*)", options=options)

            # If the user selects a file path
            if file_path:
                with open(file_path, "w") as file:
                    file.write(new_changelog)

                # Display success message
                QMessageBox.information(self, "Success", "Migration script saved successfully!")

            else:
                print("Save operation was canceled.")

        except Exception as e:
            print(f"Error generating migration script: {e}")
            # QMessageBox.critical(self, "Error", f"Error generating migration script: {e}")


        # Print the new XML to see the generated migration changelog
        # print(new_changelog)

