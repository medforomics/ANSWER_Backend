import maya


class CustomAnnotation(object):
    def __init__(self, mongo_dict={}):
        self.body = mongo_dict.get("body", "")
        self.last_updated = mongo_dict.get('lastUpdated', maya.now.iso8601())
        self.last_user = mongo_dict.get('users', '')
        self.visible = mongo_dict.get('visibility', True)

    def as_mongo_dict(self):
        mongo_dict = {'body': self.body, 'lastUpdated': self.last_updated, 'lastUser': self.last_user,
                      'visible': self.visible, }
        return mongo_dict