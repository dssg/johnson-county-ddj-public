class SimpleFeature():
    def __init__(self):
        self.description = ""
        self.num_features = 1
        self.query = None
        self.name_of_features = ""
        self.type_of_features = ""
        self.type_of_imputation = "zero"

class TimeBoundedFeature(SimpleFeature):
    def __init__(self,**kwargs):
        SimpleFeature.__init__(self)
        self.fake_today = kwargs['fake_today']
        self.db_tables = kwargs['db_tables']
