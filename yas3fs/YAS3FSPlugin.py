#!/usr/bin/python 

import imp
import os
import inspect
import logging

class YAS3FSPlugin (object):
	@staticmethod
	def load_from_file(yas3fs, filepath, expected_class = None):
		class_inst = None

		try:
			mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])
			if file_ext.lower() == '.py':
				py_mod = imp.load_source(mod_name, filepath)

			elif file_ext.lower() == '.pyc':
				py_mod = imp.load_compiled(mod_name, filepath)
			else:
				raise

			if not py_mod:
				raise

			for klass in inspect.getmembers(py_mod,inspect.isclass):
				if not issubclass(klass[1], YAS3FSPlugin):
					continue

				if expected_class == None or expected_class == klass[0]:
					class_inst = klass[1](yas3fs)
					break
		except Exception as e:
			raise Exception("cannot load plugin file " + filepath + " " + e)

		if not class_inst:
			raise Exception("cannot load plugin class " + expected_class)

		return class_inst

	@staticmethod
	def load_from_class(yas3fs, expected_class):
		try:
			module_name = 'yas3fs.'  + expected_class
			# i = imp.find_module(module_name)
			module = __import__(module_name)
			klass = getattr(module.__dict__[expected_class], expected_class)
			class_inst = klass(yas3fs)
			return class_inst
		except Exception as e:
			print(str(e))
			raise Exception("cannot load plugin class " + expected_class + " " + str(e))

	def __init__(self, yas3fs, logger=None):
		self.logger = logger
		if (not self.logger):
			self.logger = logging.getLogger('yas3fsPlugin')

	def do_cmd_on_s3_now_w_retries(self, fn):
		# self, key, pub, action, args, kargs, retries = 1
		def wrapper(*args, **kargs):
			try:	
				return fn(*args, **kargs)
			except Exception as e:
				selfless_args = None
				if args[1]:
					selfless_args = args[1:]
				self.logger.info("PLUGIN do_cmd_on_s3_now_w_retries FAILED" + " " + str(selfless_args))

			return args[2] #????
		return wrapper

