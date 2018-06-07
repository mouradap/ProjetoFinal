import sqlite3



def connect_db(db_name, logger):
    try:
        conn = sqlite3.connect(db_name + '.db')
        logger.info(f'Connetion stablished with DB: {db_name}.db')

        return conn


    except sqlite3.OperationalError:
        logger.error(f'Could not connect with {db_name}.db. Make sure the DB name is right')


def create_table(conn, logger):
    c = conn.cursor()

    try:
        c.execute('CREATE TABLE IF NOT EXISTS chipseq(cell_type_category TEXT NOT NULL, '
                  'cell_type TEXT NOT NULL,  cell_type_track_name TEXT NOT NULL, '
                  'cell_type_short TEXT NOT NULL, assay_category TEXT NOT NULL, '
                  'assay TEXT NOT NULL, assay_track_name TEXT NOT NULL, assay_short TEXT NOT NULL,'
                  'donor TEXT NOT NULL, time_point TEXT NOT NULL, view TEXT NOT NULL,'
                  'track_name TEXT NOT NULL, track_type TEXT NOT NULL, track_density TEXT NOT NULL,'
                  'provider_institution TEXT NOT NULL, source_server TEXT NOT NULL, '
                  'source_path_to_file TEXT NOT NULL, server TEXT NOT NULL, '
                  'path_to_file TEXT NOT NULL, new_file_name TEXT NOT NULL);')

        logger.info('Table chipseq was created')

    except sqlite3.OperationalError:
        logger.error('Table chipseq could not be created')


def insert_data(conn, list_of_data, logger):
    c = conn.cursor()

    try:
        with conn:
            for data in list_of_data:
                c.execute("INSERT INTO chipseq VALUES(:cell_type_category, :cell_type, :cell_type_track_name, :cell_type_short, :assay_category, :assay, :assay_track_name, :assay_short, :donor, :time_point, :view, :track_name, :track_type, :track_density, :provider_institution, :source_server, :source_path_to_file, :server, :path_to_file, :new_file_name)", data)
            logger.info('Data was inserted on the DB')

    except sqlite3.OperationalError:
        logger.error('Data could not be inserted')


def update_assay(conn, assay, donor, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE chipseq SET assay = :assay  WHERE donor = :donor", {'assay': assay, 'donor': donor})
            logger.info(f'Assay:{assay} was updated for donor: {donor}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE Assay:{assay} for donor: {donor}')


def select_celltypes(conn, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT cell_type FROM chipseq")
            all_celltypes = c.fetchall()

            logger.info(f'Selected all cell types')
            return all_celltypes

    except sqlite3.OperationalError:
        logger.error(f'Could not Select Cell types. Check if the table exists.')



def select_assay(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT cell_type_category, cell_type, cell_type_track_name, cell_type_short, assay_category, assay_track_name, assay_short, donor, time_point, view, track_name, track_type, track_density, provider_institution, source_server, source_path_to_file, server, path_to_file, new_file_name FROM chipseq WHERE assay = :assay", {"assay": assay})
            assay_all = c.fetchall()

            logger.info(f'Selected everything from assay')

            return assay_all

    except sqlite3.OperationalError:
        logger.error(f'Could not Select data from assay. Check if the table exists.')



def select_tracknames(conn, assay_track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT track_name FROM chipseq WHERE assay_track_name = :assay_track_name", {"assay_track_name": assay_track_name})
            all_tracknames = c.fetchall()

            logger.info(f'Selected trackames associated to assay: {assay_track_name}')

            return all_tracknames

    except sqlite3.OperationalError:
        logger.error(f'Could not Select tracknames. Check if the table exists.')

def select_celltypes_from_assay(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT cell_type FROM chipseq WHERE assay = :assay", {"assay": assay})
            celltypes = c.fetchall()

            logger.info(f'Selected trackames associated to assay: {assay}')

            return celltypes

    except sqlite3.OperationalError:
        logger.error(f'Could not Select tracknames. Check if the table exists.')


def delete_trackname(conn, track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("DELETE FROM chipseq WHERE track_name = :track_name", {"track_name": track_name})

            logger.info(f'Rows where track is: "{track_name}",  were deleted')

    except sqlite3.OperationalError:
        logger.error(f'Could not delete {track_name}')

