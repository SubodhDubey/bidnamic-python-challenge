from flask import Flask, jsonify, request
import sqlite3
import pandas as pd
import sqlite3 as sq

app = Flask(__name__)
DATABASE_NAME = "sqlite.db"

TOP_ALIAS_QUERY = '''
    SELECT A.alias, S.search_term, sum(S.cost) as total_cost, sum(S.conversion_value) as total_conversion_value, sum(S.conversion_value)/sum(S.cost) as roas
    FROM AdGroups A 
    INNER JOIN SearchTerms S 
    ON A.ad_group_id = S.ad_group_id
    GROUP BY A.alias, S.search_term
    ORDER BY roas DESC
    LIMIT 10
    '''


def get_db():
    conn = sqlite3.connect(DATABASE_NAME)
    return conn

@app.route('/create', methods=["GET"])
def create_tables():
    adgroups_df = pd.read_csv('adgroups.csv') 
    campaigns_df = pd.read_csv('campaigns.csv') 
    search_terms_df = pd.read_csv('search_terms.csv') 

    conn = get_db()

    adgroups_df.to_sql('AdGroups', conn, if_exists='replace', index=False) 
    campaigns_df.to_sql('Compaigns', conn, if_exists='replace', index=False) 
    search_terms_df.to_sql('SearchTerms', conn, if_exists='replace', index=False) 

    df = pd.read_sql('select * from AdGroups', conn)
    print(df)

    conn.commit()
    conn.close()
    return "Tables created Successfully" 


@app.route("/top_alias", methods=["GET"])
def top_alias():
    conn = get_db()

    adgroups_df = pd.read_sql('select * from AdGroups', conn)
    search_terms_df = pd.read_sql('select * from SearchTerms', conn)

    result_df = pd.merge(
            adgroups_df,
            search_terms_df,
            left_on="ad_group_id",
            right_on="ad_group_id",
            how="inner",
        )

    result_df = result_df.groupby(['alias', 'search_term'])['cost','conversion_value'].sum().reset_index()
    result_df['roas'] = result_df["cost"] / result_df["conversion_value"]


    result_df = result_df.sort_values(by=['roas'], ascending=False)

    # result_df =  pd.read_sql(TOP_ALIAS_QUERY, conn)
    # print(result_df)

    return jsonify(result_df.head(10).to_dict('records'))


@app.route("/top_structure_value", methods=["GET"])
def top_structure_value():
    conn = get_db()

    compaigns_df = pd.read_sql('select * from Compaigns', conn)
    search_terms_df = pd.read_sql('select * from SearchTerms', conn)

    result_df = pd.merge(
            compaigns_df,
            search_terms_df,
            left_on="campaign_id",
            right_on="campaign_id",
            how="inner",
        )

    result_df = result_df.groupby(['structure_value', 'search_term'])['cost','conversion_value'].sum().reset_index()
    result_df['roas'] = result_df["cost"] / result_df["conversion_value"]


    result_df = result_df.sort_values(by=['roas'], ascending=False)

    # result_df =  pd.read_sql(TOP_ALIAS_QUERY, conn)
    # print(result_df)

    return jsonify(result_df.head(10).to_dict('records'))


"""
Enable CORS
"""
@app.after_request
def after_request(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS, PUT, DELETE"
    response.headers["Access-Control-Allow-Headers"] = "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization"
    return response


if __name__ == "__main__":
    create_tables()
    app.run(host='0.0.0.0', port=8000, debug=True)