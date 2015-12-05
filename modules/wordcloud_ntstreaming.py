__author__ = 'Manura Omal Bhagya'

import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
import json

# tweet = "If #Obama Won't Defend " \
#         "#America Remove Him " \
#         "From Office " \
#
#         "#Obama canta jingle bells alla cerimonia di accensione dell'albero di " \
#         "#Natale - VIDEO - https://t.co/91NUNM41jg https://t.co/Q4IPs8isg6, " \
#         "RT @DrMartyFox: #ISIS #Christmas Party Attack"
#
# print tweet

def wordcloud_json(tweet):
    cv = CountVectorizer(min_df=0, stop_words="english", max_features=200,strip_accents="ascii", decode_error ="ignore")

    counts = cv.fit_transform([tweet]).toarray().ravel()
    words = np.array(cv.get_feature_names())

    counts = map(int, counts)

    words = [i.tolist() for i in words]
    words = [x.encode('UTF8') for x in words]

    dictionary = dict(zip(words, counts))

    dj = json.dumps(dictionary)
    return dj


# print wordcloud_json(tweet)