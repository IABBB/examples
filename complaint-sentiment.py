import time

from google.cloud import language_v2
import pandas as pd
import psycopg

# VARIABLES
# Number of records to retrieve
limit = 100
# BBB ID
bbbid = "0021"
# Business ID
businessid = "90233"
# Database connection string
database_connection_str = "host=*** port=5432 dbname=*** user=*** password=*** connect_timeout=10 sslmode=prefer"

print("START of analysis")

# Using Google's Natural Language API to analyze sentiment


def analyze_sentiment(text_content: str = "I am so happy and joyful.") -> None:
    client = language_v2.LanguageServiceClient()

    # Available types: PLAIN_TEXT, HTML
    document_type_in_plain_text = language_v2.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language_code = "en"
    document = {
        "content": text_content,
        "type_": document_type_in_plain_text,
        "language_code": language_code,
    }

    # Available values: NONE, UTF8, UTF16, UTF32
    # See https://cloud.google.com/natural-language/docs/reference/rest/v2/EncodingType.
    encoding_type = language_v2.EncodingType.UTF8

    response = client.analyze_sentiment(
        request={"document": document, "encoding_type": encoding_type}
    )

    return response

# Function to get complaint records from the the webapp-db01 database


def get_complaint_records(num_of_records: int = 10, bbb_id: str = "0021", business_id: str = "90233"):
    # Connect to the database
    with psycopg.connect(database_connection_str) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Query the database for the given complaint text given the bbbid and businessid
            cur.execute('SELECT fileacomplaintid, datetimesubmitted, compressedextendedtext '
                        'FROM core.datorgfileacomplaint '
                        'WHERE datetimesubmitted IS NOT NULL AND bbbid = %s AND businessid = %s '
                        'ORDER BY datetimesubmitted DESC LIMIT %s', (bbb_id, business_id, num_of_records))
            records = cur.fetchall()
            return records


# Get complaint records from database
result = get_complaint_records(limit, bbbid, businessid)

ids = []
datetime_submissions = []
# complaint_texts = []
sentiment_scores = []
sentiment_magnitudes = []

# Loop through the records and analyze sentiment of each complaint text (probably a way to do this in bulk)
for r in result:
    ids.append(r[0])
    datetime_submissions.append(r[1])
    complaint_text = r[2]
    # complaint_texts.append(complaint_text)
    # analyze sentiment
    sentiment_response = analyze_sentiment(complaint_text)
    sentiment_scores.append(sentiment_response.document_sentiment.score)
    sentiment_magnitudes.append(
        sentiment_response.document_sentiment.magnitude)

# Prepare data
df = pd.DataFrame({
    "ID": ids,
    "DateTime Submitted": datetime_submissions,
    # "Complaint Text": complaint_texts,
    "Sentiment Score": sentiment_scores,
    "Sentiment Magnitude": sentiment_magnitudes
})

# Save the data to a CSV file
df.to_csv(f"{bbbid}-{businessid}-{time.time()}.csv", index=False)

print("END of analysis")
