import argparse
import logging
import os
import sys

import util.loggerinitializer as log
from util import dbmanip as db

#Iniciando o logger:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log.initialize_logger(os.getcwd(), logger)


def main():


	parser = argparse.ArgumentParser(description = 'A Database manager using SQLite.')

	subparsers = parser.add_subparsers(title='actions',
		description='valid actions',
		help='Digite dbmanager.py {action} -h para ajuda.',
		dest='command')

	parser_index = subparsers.add_parser('createdb',
		help='Cria o banco de dados e as tabelas quando esses ainda nao existem.')

	parser_index.add_argument('--db', '--banco', dest='db', default=None,
		action='store',
		help='Um banco de dados valido.',
		required=True)

	parser_insert = subparsers.add_parser('insert', help='Insere dados nas tabelas.')

	parser_insert.add_argument('--file', default=None, action='store',
		help='Um arquivo TSV com os dados para serem inseridos no formato do banco de dados.')

	parser_insert.add_argument('--db', default=None, action='store', help='O nome do banco de dados.')

	parser_update = subparsers.add_parser('update', help='Atualiza um campo no banco de dados.')

	parser_update.add_argument("--db", default=None, action='store', help='O nome do banco de dados.')

	parser_update.add_argument("--assay", default=None, action='store', 
		help='Atualizar os assays.')

	parser_update.add_argument('--donor', default=None, action='store',
		help='Atualizar os doadores.')

	parser_select = subparsers.add_parser('select', help='Seleciona os campos do banco de dados.')

	parser_select.add_argument('--db', default=None, action='store', help='O nome do banco de dados.')

	parser_select.add_argument('--celltypes', default=None, action='store_true',
		help='Retorna todos os tipos celulares depositados no banco de dados.', required=False)

	parser_select.add_argument('--assay_all', default=None, action='store',
		help='Retorna todas as faixas de um dado assay.', required=False)

	parser_select.add_argument('--tracknames', default=None, action='store', required=False,
		help='Retorna todos os tracknames associados a um dado assay_track_name.')

	parser_select.add_argument('--assaycelltypes', default=None, action='store', required=False,
		help='Retorna todos os tipos celulares do assay.')

	parser_delete = subparsers.add_parser('delete', help='Deletar dados do banco de dados.')

	parser_delete.add_argument('--db', default=None, action='store', help='O nome do banco de dados.')

	parser_delete.add_argument('--trackname', default=None, action='store', required=False,
		help='Deleta todos os dados assocaidos a um trackname.')


	args = parser.parse_args()

	conn = db.connect_db(args.db, logger)

	if args.command == 'createdb':

		db.create_table(conn, 	logger)

	elif args.command == 'insert':
		list_of_data = []

		with open(args.file, 'r') as f:
			for line in f:
				line_dict = dict()

				if not line.strip():
					continue

				values = line.strip().split(',')

				line_dict['cell_type_category'] = values[0]
				line_dict['cell_type'] = values[1]
				line_dict['cell_type_track_name'] = values[2]
				line_dict['cell_type_short'] = values[3]
				line_dict['assay_category'] = values[4]
				line_dict['assay'] = values[5]
				line_dict['assay_track_name'] = values[6]
				line_dict['assay_short'] = values[7]
				line_dict['donor'] = values[8]
				line_dict['time_point'] = values[9]
				line_dict['view'] = values[10]
				line_dict['track_name'] = values[11]
				line_dict['track_type'] = values[12]
				line_dict['track_density'] = values[13]
				line_dict['provider_institution'] = values[14]
				line_dict['source_server'] = values[15]
				line_dict['source_path_to_file'] = values[16]
				line_dict['server'] = values[17]
				line_dict['path_to_file'] = values[18]
				line_dict['new_file_name'] = values[19]

				list_of_data.append(line_dict)

		db.insert_data(conn, list_of_data, logger)

	elif args.command == 'update':
		db.update_assay(conn, args.assay, args.donor, logger)

	elif args.command == 'select':
		if args.celltypes is True:
			all_celltypes = db.select_celltypes(conn, logger)
			for celltype in all_celltypes:
				print(celltype[0])
		if args.command == 'select' and args.assay_all is not None:
			# print(args.assay_all)
			assay_all = db.select_assay(conn, args.assay_all, logger)
			# print(assay_all)
			for assay in assay_all:
				print(assay)
		if args.command == 'select' and args.tracknames is not None:
			all_tracknames = db.select_tracknames(conn, args.tracknames, logger)
			for trackname in all_tracknames:
				print(trackname[0])
		if args.command == 'select' and args.assaycelltypes is not None:
			celltypes = db.select_celltypes_from_assay(conn, args.assaycelltypes, logger)
			for celltype in celltypes:
				print(celltype[0])


	elif args.command == 'delete':
		db.delete_trackname(conn, args.trackname, logger)


if __name__ == '__main__':
	main()
