from variantparser import dbutil as mongo

db = mongo.DbClient()
mongo.set_annotation_flags(db)
