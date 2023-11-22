from flows.web_to_gcs import parent_flow
from flows.gcs_to_bigquery import big_query


if __name__ == '__main__':
    cities = ['austin', 'amsterdam', 'barcelona', 'beijing', 'boston', 'cambridge', 'cape-town', 'chicago', 'dallas', 'broward-county']
    # parent_flow(cities)
    big_query(cities)