from xml.dom.minidom import parse, Document
import os

class ChangeLogComparator:
    def __init__(self, prev_xml_path, current_xml_path):
        self.prev_dom = parse(prev_xml_path)
        self.current_dom = parse(current_xml_path)
        self.output_dom = Document()

        # Create the root of the in-memory changelog XML
        self.output_root = self.output_dom.createElement("databaseChangeLog")
        self.output_dom.appendChild(self.output_root)

    def compare_and_generate(self):
        # Get all table elements from both changelogs
        prev_tables = self.get_tables(self.prev_dom)
        current_tables = self.get_tables(self.current_dom)

        # Check for new tables in the current XML
        self.handle_new_tables(prev_tables, current_tables)

        # Check for dropped tables in the previous XML
        self.handle_removed_tables(prev_tables, current_tables)

        # Return the generated XML in-memory as a string
        return self.output_dom.toprettyxml(indent="  ")

    def get_tables(self, dom):
        """Extracts all tables from the given DOM."""
        tables = {}
        create_table_tags = dom.getElementsByTagName("createTable")
        for table in create_table_tags:
            table_name = table.getAttribute("tableName")
            tables[table_name] = table
        return tables

    def handle_new_tables(self, prev_tables, current_tables):
        """Adds changesets for tables that are new in the current changelog."""
        for table_name, table_elem in current_tables.items():
            if table_name not in prev_tables:
                # Add a new changeset for the new table
                self.create_changeset(table_elem, "createTable")

    def handle_removed_tables(self, prev_tables, current_tables):
        """Adds changesets for tables that were removed from the current changelog."""
        for table_name, table_elem in prev_tables.items():
            if table_name not in current_tables:
                # Create a dropTable changeset for the removed table
                self.create_changeset(table_elem, "dropTable", is_remove=True)

    def create_changeset(self, table_elem, action, is_remove=False):
        """Creates a changeset with either createTable or dropTable action."""
        changeset = self.output_dom.createElement("changeset")
        changeset.setAttribute("id", f"{action}-{table_elem.getAttribute('tableName')}")
        changeset.setAttribute("author", "auto-generated")

        if action == "createTable":
            # Import the createTable tag from the current XML
            new_table_elem = self.output_dom.importNode(table_elem, True)
            changeset.appendChild(new_table_elem)
        elif action == "dropTable" and is_remove:
            # Create a dropTable tag manually
            drop_table_elem = self.output_dom.createElement("dropTable")
            drop_table_elem.setAttribute("tableName", table_elem.getAttribute("tableName"))
            changeset.appendChild(drop_table_elem)

        # Append the changeset to the root of the in-memory XML
        self.output_root.appendChild(changeset)

# # Example Usage
# # Assuming the paths to the previous and current XMLs are given
# prev_changelog_path = self.prev_xml_path
# current_changelog_path = '/path/to/current/changelog.xml'
#
# comparer = LiquibaseChangelogComparer(prev_changelog_path, current_changelog_path)
# resulting_xml = comparer.compare_and_generate()
#
# # Save the result to a file if needed
# with open("output_changelog.xml", "w") as file:
#     file.write(resulting_xml)
#
# print(resulting_xml)
