from neo4j import GraphDatabase
from neo4j.exceptions import DriverError
import traceback

# This code contains a number of workarounds for certain features / limitations of neo4j, i.e.:
# - Labels cannot be parameterised in queries
# - Not allowing constraints on relationship uniqueness, e.g.: (ref-with-title)-[:IS_ABOUT]->(trunk/branch)
#   - Check for relationship before adding one
# - Labels cannot be parameterised in queries

# And no workarounds for certain other limitations that apparently can't be worked around at the time of writing:
# - MERGE doesn't actually guarantee uniqueness: https://stackoverflow.com/questions/34302176/neo4j-merge-and-atomic-transaction
# and a uniqueness constraint cannot be applied to relationships


# TODO / WISHLIST:
# - Sort get_trunks() and get_branches() return value alphabetically
# - Adding reference to a Branch: the latter could be non-unique. Add some way to uniquely identify?
# - Get all descendants of node
# - Get all ancestors of node
# - edit_branch
# - IDs and/or keys?
# - regex matching
# - Disallow / Disable outgoing edges on Trunks
# - Disallow / Disable incoming edges on References.
# - Fetch entire graph???
# - Prevent cycles???

class TopicGraph:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth = (user, password), encrypted=True)

    def close(self):
        self.driver.close()

    def create_trunk(self, name):
        with self.driver.session() as session:
            trunk = session.write_transaction(self._create_trunk, name)
            print(f'Created trunk {trunk}')

    @staticmethod
    def _create_trunk(tx, name):
        query = (
            'MERGE (trunk:Trunk { name: $name }) '
            'RETURN trunk'
        )
        try:
            result = tx.run(query, name=name)
            return result.single()['trunk']['name']
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def get_trunks(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_trunks)
            print(f'Trunks: ')
            for row in result:
                print(f"{row['name']}")

    @staticmethod
    def _get_trunks(tx):
        query = 'MATCH (trunk:Trunk) RETURN trunk'
        try:
            result = tx.run(query)
            return [{ 'name': row['trunk']['name'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def delete_trunk(self, name):
        with self.driver.session() as session:
            session.write_transaction(self._delete_trunk, name)
            print(f'Deleted trunk {name}')

    @staticmethod
    def _delete_trunk(tx, name):
        query = (
            'MATCH (trunk:Trunk { name: $name }) '
            'DETACH DELETE trunk'
        )
        try:
            result = tx.run(query, name=name)
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def create_branch(self, name, parent_label, parent_name, note=''):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_branch, name, parent_label, parent_name, note)
            if len(result) == 0:
                print('Branch not created: it already exists, or parent label or name may be misspecified; please check.')
            else:
                for row in result:
                    print(f"Created branch {row['branch']} on parent {row['parent']}")

    @staticmethod
    def _create_branch(tx, name, parent_label, parent_name, note):
        if parent_label == 'Trunk':
            query = 'MATCH (parent:Trunk { name: $parent_name }) '
        elif parent_label == 'Branch':
            query = 'MATCH (parent:Branch { name: $parent_name }) '
        else:
            raise ValueError('Branch must be created on Trunk or Branch')
        query += (
            'WHERE NOT EXISTS { (branch:Branch { name: $name })-[:BELONGS_TO]->(parent) } '
            'WITH parent '
            'MERGE (branch:Branch { name: $name, note: $note })-[:BELONGS_TO]->(parent)'
            'RETURN branch, parent'
        )
        try:
            result = tx.run(query, name=name, parent_name=parent_name, note=note)
            return [{ 'branch': row['branch']['name'], 'parent': row['parent']['name'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def delete_branch(self, name, parent_label, parent_name):
        with self.driver.session() as session:
            result = session.write_transaction(self._delete_branch, name, parent_label, parent_name)
            print(f'Deleted branch {name} from {parent_label} {parent_name}')

    @staticmethod
    def _delete_branch(tx, name, parent_label, parent_name):
        if parent_label == 'Trunk':
            query = 'MATCH (branch:Branch { name: $name })-[:BELONGS_TO]->(parent:Trunk { name: $parent_name}) '
        elif parent_label == 'Branch':
            query = 'MATCH (branch:Branch { name: $name })-[:BELONGS_TO]->(parent:Branch { name: $parent_name}) '
        else:
            raise ValueError('Branch must belong to Trunk or Branch')
        query += 'DETACH DELETE branch'
        try:
            result = tx.run(query, name=name, parent_name=parent_name)
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    # Only branches (as opposed to trunks) can have outgoing (belonging) edges
    def connect_branch(self, from_branch, parent_label, to_parent):
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_branch, from_branch, parent_label, to_parent)
            if len(result) == 0:
                print('Connection does not exist and not created: please check correctness of node names and parent label.')
            else:
                for row in result:
                    print(f"Added connection from child {row['from']} to parent {row['to']}")

    @staticmethod
    def _connect_branch(tx, from_branch, parent_label, to_parent):
        query = 'MATCH (from:Branch { name: $from_branch }) '
        if parent_label == 'Trunk':
            query += 'MATCH (to:Trunk { name: $to_parent }) '
        elif parent_label == 'Branch':
            query += 'MATCH (to:Branch { name: $to_parent }) '
        else:
            raise ValueError('Branch must belong to Trunk or Branch')
        query += (
            'MERGE (from)-[:BELONGS_TO]->(to) '
            'RETURN from, to'
        )
        try:
            result = tx.run(query, from_branch=from_branch, to_parent=to_parent)
            return [{ 'from': row['from']['name'], 'to': row['to']['name'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def get_branches(self, parent_label, parent_name):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_branches, parent_label, parent_name)
            print(f'Branches of {parent_label} {parent_name}: ')
            for row in result:
                print(f"{row['name']}")

    @staticmethod
    def _get_branches(tx, label, name):
        if label == 'Trunk':
            query = 'MATCH (branch:Branch)-[:BELONGS_TO]->(:Trunk { name: $name }) '
        elif label == 'Branch':
            query = 'MATCH (branch:Branch)-[:BELONGS_TO]->(:Branch { name: $name }) '
        else:
            raise ValueError('Can fetch Branches only of Trunk or Branch')
        query += 'RETURN branch'
        try:
            result = tx.run(query, name=name)
            return [{ 'name': row['branch']['name'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def create_reference(self, title, url, about_label, about):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_reference, title, url, about_label, about)
            if len(result) == 0:
                msg = (
                    'Reference not added: its title is not unique for {about_label} {about}, or '
                    'topic (parent) label or title may be misspecified; please check.'
                )
                print(msg)
            else:
                for row in result:
                    print(f"Created reference {row['title']} with URL {row['url']} about {about_label} {row['about']}")

    @staticmethod
    def _create_reference(tx, title, url, about_label, about):
        if about_label == 'Trunk':
            query = 'MATCH (about:Trunk { name: $about }) '
        elif about_label == 'Branch':
            query = 'MATCH (about:Branch { name: $about }) '
        else:
            raise ValueError('Reference must pertain to Trunk or Branch')
        query += (
            'WHERE NOT EXISTS { (ref:Reference { title: $title })-[:IS_ABOUT]->(about) } '
            'WITH about '
            'MERGE (ref:Reference { title: $title, url: $url })-[:IS_ABOUT]->(about)'
            'RETURN ref, about'
        )
        try:
            result = tx.run(query, title=title, url=url, about=about)
            return [{ 'title': row['ref']['title'], 'url': row['ref']['url'], 'about': row['about']['name'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def cross_reference(self, title, current_about_label, current_about, to_label, to):
        with self.driver.session() as session:
            result = session.write_transaction(self._cross_reference, title, current_about_label, current_about, to_label, to)
            if len(result) == 0:
                print('Connection does not exist and not created: please check correctness of node names and parent label.')
            else:
                for row in result:
                    print(f"Cross referenced {row['title']} to {to_label} topic {row['to']}")

    @staticmethod
    def _cross_reference(tx, title, current_about_label, current_about, to_label, to):
        if current_about_label == 'Trunk':
            query = 'MATCH (ref:Reference { title: $title })-[:IS_ABOUT]->(:Trunk { name: $current_about }) '
        elif current_about_label == 'Branch':
            query = 'MATCH (ref:Reference { title: $title })-[:IS_ABOUT]->(:Branch { name: $current_about }) '
        else:
            raise ValueError('Reference must pertain to Trunk or Branch')
        if to_label == 'Trunk':
            query += 'MERGE (ref)-[:IS_ABOUT]->(to:Trunk { name: $to }) '
        elif to_label == 'Branch':
            query += 'MERGE (ref)-[:IS_ABOUT]->(to:Branch { name: $to }) '
        else:
            raise ValueError('Reference must pertain to Trunk or Branch')
        query += 'RETURN ref, to'
        try:
            result = tx.run(query, title=title, current_about=current_about, to=to)
            return [{ 'title': row['ref']['title'], 'to': row['to']['name'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    @staticmethod
    def _connect_topics(tx, from_branch, parent_label, to_parent):
        query = 'MATCH (from:Branch { name: $from_branch }) '
        if parent_label == 'Trunk':
            query += 'MATCH (to:Trunk { name: $to_parent }) '
        elif parent_label == 'Branch':
            query += 'MATCH (to:Branch { name: $to_parent }) '
        else:
            raise ValueError('Branch must belong to Trunk or Branch')
        query += (
            'MERGE (from)-[:BELONGS_TO]->(to) '
            'RETURN from, to'
        )
        try:
            result = tx.run(query, from_branch=from_branch, to_parent=to_parent)
            return [{ 'from': row['from']['name'], 'to': row['to']['name'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def get_references(self, about_label, about):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_references, about_label, about)
            print(f'References about {about_label} {about}:')
            for row in result:
                print(f"{row['title']}: {row['url']}")

    @staticmethod
    def _get_references(tx, about_label, about):
        if about_label == 'Trunk':
            query = 'MATCH (ref:Reference)-[:IS_ABOUT]->(:Trunk { name: $about }) '
        elif about_label == 'Branch':
            query = 'MATCH (ref:Reference)-[:IS_ABOUT]->(:Branch { name: $about }) '
        else:
            raise ValueError('Reference must belong to Trunk or Branch')
        query += 'RETURN ref'
        try:
            result = tx.run(query, about=about)
            return [{ 'title': row['ref']['title'], 'url': row['ref']['url'] } for row in result]
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

    def delete_reference(self, title, about_label, about):
        with self.driver.session() as session:
            result = session.write_transaction(self._delete_reference, title, about_label, about)
            print(f'Deleted reference {title} about {about_label} {about}')

    @staticmethod
    def _delete_reference(tx, title, about_label, about):
        if about_label == 'Trunk':
            query = 'MATCH (ref:Reference { title: $title })-[:IS_ABOUT]->(:Trunk { name: $about }) '
        elif about_label == 'Branch':
            query = 'MATCH (ref:Reference { title: $title })-[:IS_ABOUT]->(:Branch { name: $about }) '
        else:
            raise ValueError('Reference must belong to Trunk or Branch')
        try:
            result = tx.run(query + 'DETACH DELETE ref', title=title, about=about)
        except DriverError:
            print(f'{query} raised an error:\n', traceback.format_exc())
            raise

