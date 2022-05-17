import logging

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def get_news_for_user(user_id, df_ut_interactions, df_news):
    df_user1 = df_ut_interactions.loc[df_ut_interactions['user_id'] == user_id]
    items_for_user1 = df_user1['item_id'].tolist()
    x_user1 = df_news[df_news['id'].isin(items_for_user1)]
    return x_user1


def sim_score_for_user(vector, vector_user, df_news, col_name):
    cs_sim = cosine_similarity(vector, vector_user)
    cs_sim = [(x, y) for x, x_arr in enumerate(cs_sim) for y in x_arr]
    df_sim_score = pd.DataFrame(cs_sim).groupby(0).sum()
    df_sim_score.rename(columns={1: col_name}, inplace=True)
    df_sim_score[col_name] = df_sim_score[col_name] / df_sim_score[col_name].max()
    df_sim_score = df_news[['id']].join(df_sim_score)
    return df_sim_score


def content_based_score(df_ut_interactions, df_news):
    # bert_based
    logging.info("bert encoding text ")
    bert_model = SentenceTransformer('bert-base-nli-mean-tokens')
    # bert_model = SentenceTransformer('./bert_model')
    sentence_embeddings = bert_model.encode(list(df_news['combined_text']))

    # tfidf_based
    logging.info("tfidf encoding text ")
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_vectors = tfidf_vectorizer.fit_transform(df_news['combined_text'])

    logging.info("creating content-based similarity scores")
    users = df_ut_interactions.user_id.unique()
    df_content_based = df_news[['id']]
    for user_id in users:
        x_user = get_news_for_user(user_id, df_ut_interactions, df_news)
        sentence_embeddings_user = [sentence_embeddings[i] for i in x_user.index.values.tolist()]
        df_bert = sim_score_for_user(sentence_embeddings, sentence_embeddings_user, df_news, 'bert_based')
        X_user = tfidf_vectorizer.transform(x_user['combined_text'])
        df_tfidf = sim_score_for_user(tfidf_vectors, X_user, df_news, 'tfidf_based')
        df_content_based_user = df_tfidf.merge(df_bert, on='id')
        df_content_based_user[user_id] = (0.8) * df_content_based_user['tfidf_based'] + (0.2) * df_content_based_user[
            'bert_based']
        df_content_based_user = df_content_based_user.drop(columns=['tfidf_based', 'bert_based'])
        df_content_based = df_content_based.merge(df_content_based_user, on='id')
    return df_content_based
