from xml.dom import minidom

class LiquibaseChangelogComparer:
    def __init__(self, previous_xml_path, current_xml_path):
        self.previous_xml_path = previous_xml_path
        self.current_xml_path = current_xml_path
        self.change_set_counter = self.load_global_counter()

    def load_global_counter(self):
        """Load the changeset counter from the global_counter.txt file."""
        try:
            with open('global_counter.txt', 'r') as file:
                return int(file.read().strip())
        except (FileNotFoundError, ValueError):
            # If the file doesn't exist or the content is invalid, start at 1
            return 1

    def save_global_counter(self):
        """Save the current changeset counter to the global_counter.txt file."""
        with open('global_counter.txt', 'w') as file:
            file.write(str(self.change_set_counter))

    def increment_and_get_changeset_id(self, prefix):
        """Increment the changeset counter, save it, and return the new changeset ID."""
        change_set_id = f"{prefix}-{self.change_set_counter}"
        self.change_set_counter += 1
        self.save_global_counter()
        return change_set_id

    def compare_and_generate(self):
        """Main function to compare previous and current XML and generate the migration XML in memory."""
        try:
            # Load previous and current XML files
            prev_dom = minidom.parse(self.previous_xml_path)
            current_dom = minidom.parse(self.current_xml_path)

            # Get all tables and indexes from previous and current XML files
            prev_tables = prev_dom.getElementsByTagName('createTable')
            current_tables = current_dom.getElementsByTagName('createTable')

            prev_inserts = prev_dom.getElementsByTagName('insert')
            current_inserts = current_dom.getElementsByTagName('insert')

            prev_indexes = prev_dom.getElementsByTagName('createIndex')
            current_indexes = current_dom.getElementsByTagName('createIndex')

            # Create an in-memory XML structure for migration script
            in_memory_xml = self.create_in_memory_xml()

            # Handle table additions or deletions
            self.handle_create_table_changes(prev_tables, current_tables, in_memory_xml)

            # Handle column changes (added/dropped columns)
            self.handle_column_changes(prev_tables, current_tables, in_memory_xml)

            # Handle <insert> changes
            self.handle_insert_changes(prev_inserts, current_inserts, in_memory_xml)

            # Handle <createIndex> and <dropIndex> changes
            self.handle_index_changes(prev_indexes, current_indexes, in_memory_xml)

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
        for current_table in current_tables:
            current_table_name = current_table.getAttribute('tableName')
            prev_table = self.get_table_by_name(prev_tables, current_table_name)
            if not prev_table:
                changeset = in_memory_xml.createElement('changeSet')
                changeset.setAttribute('author', 'migration')
                changeset.setAttribute('id', self.increment_and_get_changeset_id(f'create-table-{current_table_name}'))

                cloned_table = current_table.cloneNode(True)
                changeset.appendChild(cloned_table)
                in_memory_xml.documentElement.appendChild(changeset)

        for prev_table in prev_tables:
            prev_table_name = prev_table.getAttribute('tableName')
            current_table = self.get_table_by_name(current_tables, prev_table_name)
            if not current_table:
                changeset = in_memory_xml.createElement('changeSet')
                changeset.setAttribute('author', 'migration')
                changeset.setAttribute('id', self.increment_and_get_changeset_id(f'drop-table-{prev_table_name}'))

                drop_table = in_memory_xml.createElement('dropTable')
                drop_table.setAttribute('tableName', prev_table_name)
                changeset.appendChild(drop_table)
                in_memory_xml.documentElement.appendChild(changeset)

    def handle_column_changes(self, prev_tables, current_tables, in_memory_xml):
        """Handle column changes (additions, deletions) between previous and current XML."""
        try:
            for current_table in current_tables:
                current_table_name = current_table.getAttribute('tableName')
                prev_table = self.get_table_by_name(prev_tables, current_table_name)

                if prev_table:
                    current_columns = current_table.getElementsByTagName('column')
                    prev_columns = prev_table.getElementsByTagName('column')

                    added_columns = [col for col in current_columns if not self.column_exists_in_table(prev_columns, col)]
                    if added_columns:
                        add_column_changeset = in_memory_xml.createElement('changeSet')
                        add_column_changeset.setAttribute('author', 'migration')
                        add_column_changeset.setAttribute('id', self.increment_and_get_changeset_id(f'add-column-{current_table_name}'))

                        add_column_tag = in_memory_xml.createElement('addColumn')
                        add_column_tag.setAttribute('tableName', current_table_name)

                        for column in added_columns:
                            add_column_tag.appendChild(column.cloneNode(True))

                        add_column_changeset.appendChild(add_column_tag)
                        in_memory_xml.documentElement.appendChild(add_column_changeset)

            for prev_table in prev_tables:
                prev_table_name = prev_table.getAttribute('tableName')
                current_table = self.get_table_by_name(current_tables, prev_table_name)

                if current_table:
                    prev_columns = prev_table.getElementsByTagName('column')
                    current_columns = current_table.getElementsByTagName('column')

                    dropped_columns = [col for col in prev_columns if not self.column_exists_in_table(current_columns, col)]
                    if dropped_columns:
                        drop_column_changeset = in_memory_xml.createElement('changeSet')
                        drop_column_changeset.setAttribute('author', 'migration')
                        drop_column_changeset.setAttribute('id', self.increment_and_get_changeset_id(f'drop-column-{prev_table_name}'))

                        drop_column_tag = in_memory_xml.createElement('dropColumn')
                        drop_column_tag.setAttribute('tableName', prev_table_name)

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
        for curr_insert in curr_inserts:
            table_name = curr_insert.getAttribute("tableName")
            prev_insert_found = False

            for prev_insert in prev_inserts:
                if prev_insert.getAttribute("tableName") == table_name:
                    prev_insert_found = True
                    break

            if not prev_insert_found:
                change_set = in_memory_xml.createElement("changeSet")
                change_set.setAttribute("author", "migration")
                change_set.setAttribute("id", self.increment_and_get_changeset_id(f'insert-{table_name}'))

                insert_tag = in_memory_xml.createElement("insert")
                insert_tag.setAttribute("tableName", table_name)

                for column in curr_insert.getElementsByTagName("column"):
                    column_copy = column.cloneNode(True)
                    insert_tag.appendChild(column_copy)

                change_set.appendChild(insert_tag)
                in_memory_xml.documentElement.appendChild(change_set)

    def handle_index_changes(self, prev_indexes, current_indexes, in_memory_xml):
        """Handle comparison of createIndex and dropIndex between two XMLs."""
        for curr_index in current_indexes:
            table_name = curr_index.getAttribute("tableName")
            index_name = curr_index.getAttribute("indexName")
            prev_index_found = False

            for prev_index in prev_indexes:
                if prev_index.getAttribute("tableName") == table_name and prev_index.getAttribute("indexName") == index_name:
                    prev_index_found = True
                    break

            if not prev_index_found:
                # Add new createIndex changeset
                change_set = in_memory_xml.createElement("changeSet")
                change_set.setAttribute("author", "migration")
                change_set.setAttribute("id", self.increment_and_get_changeset_id(f'create-index-{table_name}-{index_name}'))

                cloned_index = curr_index.cloneNode(True)
                change_set.appendChild(cloned_index)
                in_memory_xml.documentElement.appendChild(change_set)

        for prev_index in prev_indexes:
            table_name = prev_index.getAttribute("tableName")
            index_name = prev_index.getAttribute("indexName")
            curr_index_found = False

            for curr_index in current_indexes:
                if curr_index.getAttribute("tableName") == table_name and curr_index.getAttribute("indexName") == index_name:
                    curr_index_found = True
                    break

            if not curr_index_found:
                # Add dropIndex changeset for indexes present in prev XML but missing in current XML
                drop_changeset = in_memory_xml.createElement('changeSet')
                drop_changeset.setAttribute('author', 'migration')
                drop_changeset.setAttribute('id', self.increment_and_get_changeset_id(f'drop-index-{table_name}-{index_name}'))

                drop_index = in_memory_xml.createElement('dropIndex')
                drop_index.setAttribute('indexName', index_name)
                drop_index.setAttribute('tableName', table_name)
                drop_changeset.appendChild(drop_index)

                in_memory_xml.documentElement.appendChild(drop_changeset)

    def get_table_by_name(self, tables, table_name):
        """Return the table element with the specified name, or None if not found."""
        for table in tables:
            if table.getAttribute('tableName') == table_name:
                return table
        return None

    def column_exists_in_table(self, table_columns, column):
        """Check if a column exists in the table's list of columns."""
        column_name = column.getAttribute('name')
        for table_column in table_columns:
            if table_column.getAttribute('name') == column_name:
                return True
        return False

# # Usage example
# previous_xml_path = 'previous_changelog.xml'
# current_xml_path = 'current_changelog.xml'
# comparer = LiquibaseChangelogComparer(previous_xml_path, current_xml_path)
# migration_script = comparer.compare_and_generate()
#
# if migration_script:
#     print(migration_script)
