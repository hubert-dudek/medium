# Databricks notebook source
# MAGIC %md
# MAGIC [https://docs.databricks.com/api/azure/workspace/genie](https://docs.databricks.com/api/azure/workspace/genie)

# COMMAND ----------

import requests

# COMMAND ----------

access_token = "faketoken"
workspace_url = "https://fakeid.cloud.databricks.com//" 
genie_room_id = "01f001bbf8bb106fb2e54ab399c1f36e?o=3323272837455455" # egg shop room id from the url

# COMMAND ----------

# 1. getspace: Retrieve available spaces
get_spaces_url = f"{workspace_url}/api/2.0/genie/spaces/{genie_room_id}"
response1 = requests.get(get_spaces_url, headers={"Authorization": f"Bearer {access_token}"})
print(response1.text)

# COMMAND ----------

# 2. startconversation: Start a new conversation in a specific space
start_conv_url = f"{workspace_url}/api/2.0/genie/spaces/{genie_room_id}/start-conversation"
payload_start = {"content": "Hi egg shop!"}
response2 = requests.post(start_conv_url, headers={"Authorization": f"Bearer {access_token}"}, json=payload_start)
print(response2.text)

# COMMAND ----------

# 3. createmessage: Create a new message in the conversation
conversation_id = response2.json()['conversation_id']
create_msg_url = f"{workspace_url}/api/2.0/genie/spaces/{genie_room_id}/conversations/{conversation_id}/messages"
payload_message = {"content": "How many eggs do I have?"}
response3 = requests.post(create_msg_url, headers={"Authorization": f"Bearer {access_token}"}, json=payload_message)
print(response3.text)

# COMMAND ----------

# 4. getmessage: Retrieve the created message from the conversation
message_id = response3.json()['message_id']
get_msg_url = f"{workspace_url}/api/2.0/genie/spaces/{genie_room_id}/conversations/{conversation_id}/messages/{message_id}"
response4 = requests.get(get_msg_url, headers={"Authorization": f"Bearer {access_token}"})
print(response4.text)

# COMMAND ----------

# 5. getmessageattachmentqueryresult: Get the SQL query attachment result (if any) for the message
attachment_id = response4.json()['attachments'][0]['attachment_id']
get_attachment_result_url = f"{workspace_url}/api/2.0/genie/spaces/{genie_room_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
response5 = requests.get(get_attachment_result_url, headers={"Authorization": f"Bearer {access_token}"})
print(response5.text)
# here in data_array we see how many eggs there are - 1000



# COMMAND ----------

# 6. we can also use SQL endpoint statement info to get that result
statement_id = response4.json()['query_result']['statement_id']
get_attachment_result_url = f"{workspace_url}/api/2.0/sql/statements/{statement_id}"
response6 = requests.get(get_attachment_result_url, headers={"Authorization": f"Bearer {access_token}"})
print(response6.text)

# COMMAND ----------

# 7. executemessageattachmentquery: Re-execute the SQL query attachment (if needed)
exec_query_url = f"{workspace_url}/api/2.0/genie/spaces/{genie_room_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/execute-query"
response7 = requests.post(exec_query_url, headers={"Authorization": f"Bearer {access_token}"})
print(response7.text)
