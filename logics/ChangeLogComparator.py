from xml.dom import minidom

# Global variable to track the unique ID counter for change sets
change_set_counter = 0

class LiquibaseChangelogComparer:
    def __init__(self, previous_xml_path, current_xml_path):
        self.previous_xml_path = previous_xml_path
        self.current_xml_path = current_xml_path

    def compare_and_generate(self):
        """Main function to compare previous and current XML and generate the migration XML in memory."""
        try:
            # Load previous and current XML files
            prev_dom = minidom.parse(self.previous_xml_path)
            current_dom = minidom.parse(self.current_xml_path)

            # Get all tables from previous and current XML files
            prev_tables = prev_dom.getElementsByTagName('createTable')
            current_tables = current_dom.getElementsByTagName('createTable')

            prev_inserts = prev_dom.getElementsByTagName('insert')
            current_inserts = current_dom.getElementsByTagName('insert')

            # Create an in-memory XML structure for migration script
            in_memory_xml = self.create_in_memory_xml()

            # Handle table additions or deletions
            self.handle_create_table_changes(prev_tables, current_tables, in_memory_xml)

            # Handle column changes (added/dropped columns)
            self.handle_column_changes(prev_tables, current_tables, in_memory_xml)

            # Handle <insert> changes
            self.handle_insert_changes(prev_inserts, current_inserts, in_memory_xml)

            # Return the generated in-memory XML as a string
            return in_memory_xml.toprettyxml(indent="  ")

        except Exception as e:
            print(f"Error generating migration script: {e}")
            return None

    def create_in_memory_xml(self):
        """Creates the in-memory XML structure with the root element."""
        doc = minidom.Document()

        # Create the root element
        database_change_log = doc.createElement('databaseChangeLog')
        database_change_log.setAttribute('xmlns', 'http://www.liquibase.org/xml/ns/dbchangelog')
        database_change_log.setAttribute('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
        database_change_log.setAttribute('xsi:schemaLocation',
                                         'http://www.liquibase.org/xml/ns/dbchangelog '
                                         'http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd')
        doc.appendChild(database_change_log)

        return doc

    def handle_create_table_changes(self, prev_tables, current_tables, in_memory_xml):
        """Handle table changes (additions, deletions) between previous and current XML."""
        global change_set_counter

        # Identify added tables
        for current_table in current_tables:
            current_table_name = current_table.getAttribute('tableName')
            prev_table = self.get_table_by_name(prev_tables, current_table_name)
            if not prev_table:
                # Table is new, create a changeSet for createTable
                changeset = in_memory_xml.createElement('changeSet')
                change_set_counter += 1
                changeset.setAttribute('author', 'migration')
                changeset.setAttribute('id', f'create-table-{current_table_name}-{change_set_counter}')

                # Clone the current_table element and add to the changeset
                cloned_table = current_table.cloneNode(True)
                changeset.appendChild(cloned_table)
                in_memory_xml.documentElement.appendChild(changeset)

        # Identify deleted tables
        for prev_table in prev_tables:
            prev_table_name = prev_table.getAttribute('tableName')
            current_table = self.get_table_by_name(current_tables, prev_table_name)
            if not current_table:
                # Table is deleted, create a changeSet for dropTable
                changeset = in_memory_xml.createElement('changeSet')
                change_set_counter += 1
                changeset.setAttribute('author', 'migration')
                changeset.setAttribute('id', f'drop-table-{prev_table_name}-{change_set_counter}')

                # Create a dropTable element
                drop_table = in_memory_xml.createElement('dropTable')
                drop_table.setAttribute('tableName', prev_table_name)
                changeset.appendChild(drop_table)
                in_memory_xml.documentElement.appendChild(changeset)

    def handle_column_changes(self, prev_tables, current_tables, in_memory_xml):
        """Handle column changes (additions, deletions) between previous and current XML."""
        try:
            global change_set_counter

            # Loop through current tables to identify added columns
            for current_table in current_tables:
                current_table_name = current_table.getAttribute('tableName')
                prev_table = self.get_table_by_name(prev_tables, current_table_name)

                # If the table exists in both XMLs, check column differences
                if prev_table:
                    current_columns = current_table.getElementsByTagName('column')
                    prev_columns = prev_table.getElementsByTagName('column')

                    # Find columns that are in current XML but not in the previous XML (added columns)
                    added_columns = [col for col in current_columns if not self.column_exists_in_table(prev_columns, col)]
                    if added_columns:
                        # Create addColumn changeSet
                        add_column_changeset = in_memory_xml.createElement('changeSet')
                        change_set_counter += 1
                        add_column_changeset.setAttribute('author', 'migration')
                        add_column_changeset.setAttribute('id', f'add-column-{current_table_name}-{change_set_counter}')

                        add_column_tag = in_memory_xml.createElement('addColumn')
                        add_column_tag.setAttribute('tableName', current_table_name)

                        # Add each missing column to the addColumn tag
                        for column in added_columns:
                            add_column_tag.appendChild(column.cloneNode(True))

                        add_column_changeset.appendChild(add_column_tag)
                        in_memory_xml.documentElement.appendChild(add_column_changeset)

            # Loop through previous tables to identify dropped columns
            for prev_table in prev_tables:
                prev_table_name = prev_table.getAttribute('tableName')
                current_table = self.get_table_by_name(current_tables, prev_table_name)

                # If the table exists in both XMLs, check column differences
                if current_table:
                    prev_columns = prev_table.getElementsByTagName('column')
                    current_columns = current_table.getElementsByTagName('column')

                    # Find columns that are in previous XML but not in the current XML (dropped columns)
                    dropped_columns = [col for col in prev_columns if not self.column_exists_in_table(current_columns, col)]
                    if dropped_columns:
                        # Create dropColumn changeSet
                        drop_column_changeset = in_memory_xml.createElement('changeSet')
                        change_set_counter += 1
                        drop_column_changeset.setAttribute('author', 'migration')
                        drop_column_changeset.setAttribute('id', f'drop-column-{prev_table_name}-{change_set_counter}')

                        drop_column_tag = in_memory_xml.createElement('dropColumn')
                        drop_column_tag.setAttribute('tableName', prev_table_name)

                        # Add each dropped column to the dropColumn tag
                        for column in dropped_columns:
                            column_name = column.getAttribute('name')
                            column_element = in_memory_xml.createElement('column')
                            column_element.setAttribute('name', column_name)

                            drop_column_tag.appendChild(column_element)

                        drop_column_changeset.appendChild(drop_column_tag)
                        in_memory_xml.documentElement.appendChild(drop_column_changeset)

        except Exception as e:
            print(f"Error while handling column changes: {e}")

    def handle_insert_changes(self, prev_inserts, curr_inserts, in_memory_xml):
        """Handle comparison of insert statements between two XMLs."""
        global change_set_counter

        # Iterate through the insert tags in the current XML
        for curr_insert in curr_inserts:
            table_name = curr_insert.getAttribute("tableName")
            prev_insert_found = False

            # Check if the insert exists in the previous XML for the same table
            for prev_insert in prev_inserts:
                if prev_insert.getAttribute("tableName") == table_name:
                    prev_insert_found = True
                    break

            # If the insert is not in the previous XML, add it to the in-memory XML
            if not prev_insert_found:
                change_set_counter += 1
                change_set = in_memory_xml.createElement("changeSet")
                change_set.setAttribute("author", "migration")
                change_set.setAttribute("id", f"insert-{table_name}-{change_set_counter}")

                insert_tag = in_memory_xml.createElement("insert")
                insert_tag.setAttribute("tableName", table_name)

                # Copy columns from the current insert
                for column in curr_insert.getElementsByTagName("column"):
                    column_copy = column.cloneNode(True)
                    insert_tag.appendChild(column_copy)

                change_set.appendChild(insert_tag)
                in_memory_xml.documentElement.appendChild(change_set)

        # Inserts that are in prev_xml but not in curr_xml will not be added

    def get_table_by_name(self, tables, table_name):
        """Utility function to get a table element by its tableName attribute."""
        for table in tables:
            if table.getAttribute('tableName') == table_name:
                return table
        return None

    def column_exists_in_table(self, columns, column):
        """Utility function to check if a column exists in a table."""
        column_name = column.getAttribute('name')
        for col in columns:
            if col.getAttribute('name') == column_name:
                return True
        return False
