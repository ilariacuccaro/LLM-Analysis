from onprem import LLM
import os
import json
import re
from pymongo import MongoClient

def get_number_and_content(csv_data):
    # Matches article number in "Article number: ( d+)"
    match = re.search(r'Number article: (\d+)', csv_data)
    if match:
        # Extract the found number
        article_number = match.group(1)
    else:
        print("ERROR: Could not identify text, add Number Article")
        return "500", "error",""
      
    match = re.search(r'Title: (.*?)\nContent:', csv_data, re.DOTALL)
    if match:
        title = match.group(1)        
    else:
        ("ERROR: Title cannot be identified")    
        return "500", "error",""
     
    # Find the index where it starts "Title:"
    start_index = csv_data.find("Content:")
    # If "Title:" is present, you get the substring following "Title:  
    if start_index != -1:
        content = csv_data[start_index + len("Content:"):]
    else:
        print("ERROR: Text cannot be identified")
        return "500", "error",""
    
    return article_number, title, content


llm = LLM()

current_directory = os.getcwd()
file_path_dataset = os.path.join(current_directory, "Lib", "site-packages", "onprem", "simple_data")
llm.ingest(file_path_dataset)

file_path_questions = os.path.join(current_directory, "Lib", "site-packages", "onprem", "questions", "questions.json")
# Open the JSON file and upload the content
with open(file_path_questions, 'r') as file:
    content = json.load(file)

all_question = content["questions"]

result = {}  # dictionary used to store results

for question in all_question:
    result_question = llm.ask(question)

    for index,doc in enumerate (result_question["source_documents"]):
        csv_data = doc.page_content
        article_number, title ,content = get_number_and_content(csv_data)
        if title == "error":
            continue
        
        article_key = "number_article_" + article_number
        
        question_answer = {
            "question": result_question["question"],
            "answer": result_question["answer"][index]
            }
        
        # If the key does not exist in the result dictionary, it is created and initialized with the title, content and an empty list of questions and answers.
        if(article_key not in result):
            result[article_key] = {
                "title": title,
                "content": content,
                "question_answer": []
            }
        result[article_key]["question_answer"].append(question_answer) # the question-answer pair is added to the question_answer list in the corresponding result dictionary

print("output: ",json.dumps(result)) # Printing the final result in JSON format

# Specifies the location of the JSON file where you want to save the contents of the Result variable
file_path_output = os.path.join(current_directory, "output.json")

# Open the file in write mode and use json.dump() to write the contents of the dictionary to the JSON file
with open(file_path_output, "w") as file:
    json.dump(result, file)


def send_to_mongodb(file_path_output, mongodb_uri, database_name, collection_name):
    # Connection to mongodb
    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    with open(file_path_output, 'r') as file:
        json_data = json.load(file)

        documents_to_insert = list(json_data.values())
        inserted_ids = collection.insert_many(documents_to_insert)
        print(f"Inserted {len(inserted_ids.inserted_ids)} documents into MongoDB.")

mongodb_uri = "mongodb+srv://bigdata:ProjectBigData123@llm.gsegixp.mongodb.net/"  
database_name = "BigData"
collection_name = "Output"

send_to_mongodb(file_path_output, mongodb_uri, database_name, collection_name)
