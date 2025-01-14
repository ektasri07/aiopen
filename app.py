import requests
from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from botbuilder.schema import Activity, ChannelAccount
import pandas as pd
from azure.identity import DefaultAzureCredential
import openai
import logging
import os

class AzureCostBot(ActivityHandler):
    def __init__(self):
        super().__init__()
        self.openai_endpoint = os.getenv("OPENAI_ENDPOINT")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_deployment_name = os.getenv("OPENAI_DEPLOYMENT_NAME")

        # Set up OpenAI API configuration
        openai.api_key = self.openai_api_key
        openai.api_base = self.openai_endpoint
        openai.api_type = "azure"
        openai.api_version = "2024-12-01-preview"

    async def on_members_added_activity(self, members_added: [ChannelAccount], turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Welcome to the Azure Subscription Cost Chatbot! Ask me about Azure costs.")

    async def on_message_activity(self, turn_context: TurnContext):
        user_input = turn_context.activity.text
        try:
            # Call Azure OpenAI API to interpret the query
            openai_response = await self.get_openai_response(user_input)
            query_info = self.extract_query_info(openai_response)

            # Get cost data based on the interpreted query
            cost_data = self.get_cost_data(query_info)
            df = self.process_cost_data(cost_data, query_info)
            await turn_context.send_activity(MessageFactory.text(df.to_string()))
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            await turn_context.send_activity(f"An error occurred: {str(e)}")

    def get_cost_data(self, query_info):
        credential = DefaultAzureCredential()
        cost_mgmt_url = "https://management.azure.com/subscriptions/7b9338d2-e8dc-405b-91d7-ef8fe30b97b6/providers/Microsoft.CostManagement/query?api-version=2021-01-01"

        headers = {
            'Authorization': f'Bearer {credential.get_token("https://management.azure.com/.default").token}',
            'Content-Type': 'application/json'
        }

        # Define the request body for the cost query
        body = {
            "type": "Usage",
            "timeframe": "Custom",
            "timePeriod": {
                "from": query_info['start_date'],
                "to": query_info['end_date']
            },
            "dataset": {
                "granularity": query_info['granularity'],
                "aggregation": {
                    "totalCost": {
                        "name": "Cost",
                        "function": "Sum"
                    }
                },
                "filter": {
                    "dimensions": {
                        "name": "ResourceGroupName",
                        "operator": "In",
                        "values": query_info['resource_groups']
                    }
                } if query_info['resource_groups'] else {}
            }
        }

        response = requests.post(cost_mgmt_url, headers=headers, json=body)
        response.raise_for_status()
        return response.json()

    def process_cost_data(self, cost_data, query_info):
        data = []
        properties = cost_data.get('properties', {})
        rows = properties.get('rows', [])
        for item in rows:
            data.append({
                'Date': item[1],
                'Cost': item[0]
            })

        df = pd.DataFrame(data)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df.sort_index(inplace=True)

        if query_info['granularity'] == 'Daily':
            return df
        else:
            return df.resample('M').sum()

    async def get_openai_response(self, prompt):
        response = openai.ChatCompletion.create(
            engine=self.openai_deployment_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message['content']

    def extract_query_info(self, openai_response):
        # Extract relevant information from the OpenAI response
        # This is a placeholder implementation and should be adapted based on the actual response format
        query_info = {
            'start_date': '2023-01-01',
            'end_date': '2023-01-31',
            'granularity': 'Daily',
            'resource_groups': []
        }
        return query_info

if __name__ == "__main__":
    from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings
    from botbuilder.integration.aiohttp import BotFrameworkHttpClient, BotFrameworkHttpAdapter
    from aiohttp import web

    settings = BotFrameworkAdapterSettings("", "")
    adapter = BotFrameworkHttpAdapter(settings)

    bot = AzureCostBot()

    async def messages(req):
        body = await req.json()
        activity = Activity().deserialize(body)
        auth_header = req.headers.get("Authorization", "")
        response = await adapter.process_activity(activity, auth_header, bot.on_turn)
        return web.json_response(data=response.body, status=response.status)

    app = web.Application()
    app.router.add_post("/api/messages", messages)

    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
