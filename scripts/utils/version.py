import scripts
import modules
from DigInEngine import __version__ as main_version

version_dict = {}
def get_version():
    version_list = [{'DigInEngine' : main_version},
                    {scripts.LogicImplementer.LogicImplementer.__name__ : scripts.LogicImplementer.LogicImplementer.__version__},
                    {scripts.AggregationEnhancer.AggregationEnhancer.__name__ : scripts.AggregationEnhancer.AggregationEnhancer.__version__},
                    {scripts.DataSourceService.DataSourceService.__name__ : scripts.DataSourceService.DataSourceService.__version__},
                    {scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.__name__ : scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.__version__,
                     scripts.DescriptiveAnalyticsService.descriptive_processor.__name__: scripts.DescriptiveAnalyticsService.descriptive_processor.__version__},
                    {scripts.DiginAlgo.DiginAlgo_service.__name__ : scripts.DiginAlgo.DiginAlgo_service.__version__},
                    {scripts.DigInRatingEngine.DigInRatingEngine.__name__ : scripts.DigInRatingEngine.DigInRatingEngine.__version__,
                     scripts.DigInRatingEngine.ExceedUsageCalculator.__name__: scripts.DigInRatingEngine.ExceedUsageCalculator.__version__},
                    {scripts.FileUploadService.FileUploadService.__name__ : scripts.FileUploadService.FileUploadService.__version__,
                     scripts.FileUploadService.FileDatabaseInsertionCSV.__name__: scripts.FileUploadService.FileDatabaseInsertionCSV.__version__,
                     scripts.FileUploadService.GetTableSchema.__name__: scripts.FileUploadService.GetTableSchema.__version__},
                    {scripts.DigINCacheEngine.CacheController.__name__ : scripts.DigINCacheEngine.CacheController.__version__,
                     scripts.DigINCacheEngine.CacheGarbageCleaner.__name__: scripts.DigINCacheEngine.CacheGarbageCleaner.__version__},
                    {scripts.DiginComponentStore.DiginComponentStore.__name__ : scripts.DiginComponentStore.DiginComponentStore.__version__},
                    {scripts.PackageHandlingService.PackageHandlingService.__name__ : scripts.PackageHandlingService.PackageHandlingService.__version__},
                    {scripts.UserManagementService.UserMangementService.__name__ : scripts.UserManagementService.UserMangementService.__version__},
                    {scripts.SocialMediaService.SocialMediaService.__name__ : scripts.SocialMediaService.SocialMediaService.__version__},
                    {scripts.PentahoReportingService.PentahoReportingService.__name__ : scripts.PentahoReportingService.PentahoReportingService.__version__,
                     scripts.PentahoReportingService.ReportInitialConfig.__name__: scripts.PentahoReportingService.ReportInitialConfig.__version__},
                    {scripts.PredictiveAnalysisEngine.ForecastingEsService.__name__ : scripts.PredictiveAnalysisEngine.ForecastingEsService.__version__,
                     scripts.PredictiveAnalysisEngine.ForecastingEsProcessor.__name__: scripts.PredictiveAnalysisEngine.ForecastingEsProcessor.__version__},
                    {scripts.ShareComponentService.ShareComponentService.__name__ : scripts.ShareComponentService.ShareComponentService.__version__},
                    {scripts.DigInScheduler.DigInScheduler.__name__ : scripts.DigInScheduler.DigInScheduler.__version__},
                    {scripts.utils.AuthHandler.__name__ : scripts.utils.AuthHandler.__version__,
                     scripts.utils.DiginIDGenerator.__name__ : scripts.utils.DiginIDGenerator.__version__,
                     scripts.utils.GetSystemDirectories.__name__ : scripts.utils.GetSystemDirectories.__version__},
                    {modules.BigQueryHandler.__name__ : modules.BigQueryHandler.__version__},
                    {modules.MySQLhandler.__name__ : modules.MySQLhandler.__version__},
                    {modules.SQLQueryHandler.__name__ : modules.SQLQueryHandler.__version__},
                    {modules.PostgresHandler.__name__ : modules.PostgresHandler.__version__},
                    {modules.BigQueryHandler.__name__ : modules.BigQueryHandler.__version__},
                    #{modules.AgglomerativeClustering.__name__ : modules.AgglomerativeClustering.__version__},
                    {modules.bipartite.__name__ : modules.bipartite.__version__},
                    {modules.Boxplot.__name__ : modules.Boxplot.__version__},
                    {modules.Bubblechart.__name__ : modules.Bubblechart.__version__},
                    {modules.CommonMessageGenerator.__name__ : modules.CommonMessageGenerator.__version__},
                    {modules.DoubleExponentialSmoothing.__name__ : modules.DoubleExponentialSmoothing.__version__},
                    #{modules.FrequentPatternGrowth.__name__ : modules.FrequentPatternGrowth.__version__},
                    {modules.Bubblechart.__name__ : modules.Bubblechart.__version__},
                    {modules.Histogram.__name__ : modules.Histogram.__version__},
                    #{modules.HoltWinter.__name__ : modules.HoltWinter.__version__},
                    {modules.kmeans_algo.__name__ : modules.kmeans_algo.__version__},
                    {modules.linearRegression.__name__ : modules.linearRegression.__version__},
                    {modules.sentimentAnalysis.__name__ : modules.sentimentAnalysis.__version__},
                    {modules.SocialMediaAuthHandler.__name__ : modules.SocialMediaAuthHandler.__version__}


                    ]
    # for name, val in scripts.__dict__.iteritems():
    #     print name.__dict__
    #     print val
    return version_list