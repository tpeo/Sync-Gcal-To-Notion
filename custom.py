from __future__ import print_function
import pytz, asyncio, json, os.path, os, secrets
from aiogoogle import Aiogoogle
from datetime import datetime, date, timedelta
from notion.client import NotionClient
from notion.collection import NotionDate

##google credentials + info
service_account_creds = {
    "scopes": [
        "https://www.googleapis.com/auth/calendar"
    ],
    **json.load(open('./credentials.json'))
}
gcal_id = secrets.gcal_id

##notion credentials + info
notion_token = secrets.notion_token
notion_cal_link = secrets.notion_cal_link
timezone = 'America/Chicago'


def main():
    asyncio.run(handler())

async def handler():
    notion_events = get_notion_events()
    async with Aiogoogle(service_account_creds=service_account_creds) as google:
        #connect to gcal api
        gcal_api = await google.discover("calendar", "v3")
        events_request = gcal_api.events.list(calendarId=gcal_id, singleEvents=True, timeMin=get_iso_timestamp())
        events = await google.as_service_account(events_request)
        async_tasks = []
        #find all necessary tasks by matching notion to gcal events
        for notion_event in notion_events:
            gcal_match = [x for x in events['items'] if x['id'] == notion_event['id']]
            new_gcal_event = format_notion_event_for_gcal(notion_event)
            #there is a match!
            if len(gcal_match) == 1:
                gcal_match = gcal_match[0]
                #check if data is the same
                gcal_match = {k:gcal_match[k] for k in new_gcal_event if k in gcal_match}
                if not same_events(gcal_match, new_gcal_event):
                    print('updating event')
                    #update existing event in gcal
                    req = gcal_api.events.update(calendarId=gcal_id, eventId=gcal_match['id'], json=format_notion_event_for_gcal(notion_event))
                    await google.as_service_account(req)
            #no match
            elif len(gcal_match) == 0:
                print('creating new event')
                #create event in gcal
                req = gcal_api.events.insert(calendarId=gcal_id, json=format_notion_event_for_gcal(notion_event))
                await google.as_service_account(req)
        #find gcal events to delete
        notion_ids = [x['id'] for x in notion_events]
        deleted_events = [x['id'] for x in events['items'] if x['id'] not in notion_ids]
        for old_event_id in deleted_events:
            print('deleting event')
            req = gcal_api.events.delete(calendarId=gcal_id, eventId=old_event_id)
            await google.as_service_account(req)
        #run all tasks asynchronously
        # await asyncio.gather(*async_tasks)

def get_iso_timestamp(timezone="America/Chicago"):
    return datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(timezone)).isoformat()

def same_events(event1, event2):
    def rfc_to_datetime(rfc_str):
        return datetime.strptime(''.join(rfc_str.rsplit(':', 1)), '%Y-%m-%dT%H:%M:%S%z')
    for k,v in event1.items():
        if k=='start':
            diff_date = rfc_to_datetime(event1['start']['dateTime']) - rfc_to_datetime(event2['start']['dateTime'])
            if abs(diff_date.total_seconds()) > 1:
                return False
        elif k=='end':
            diff_date = rfc_to_datetime(event1['end']['dateTime']) - rfc_to_datetime(event2['end']['dateTime'])
            if abs(diff_date.total_seconds()) > 1:
                return False
        elif event1[k] != event2[k]:
            return False
    return True

def get_notion_events():
    # Grab all events in Notion cal
    client = NotionClient(token_v2=notion_token)
    notion_cal = client.get_collection_view(notion_cal_link)

    filter_params = {
        "filters": [{"property":"title","filter":{"operator":"is_not_empty"}},{"property":"D{iX","filter":{"operator":"is_not_empty"}},{"property":"uSQK","filter":{"operator":"date_is_on_or_after","value":{"type":"relative","value":"today"}}}],
        "operator": "and"
    }
    all_rows = notion_cal.build_query(filter=filter_params).execute()

    #formats dates for gcal
    def dates_for_gcal(date):
        notion_start = date.start
        start = str(notion_start).replace(" ", "T")
        notion_end = date.end
        end = ''
        if " " not in str(notion_end): # if it's just a date without a time
            end = str(notion_end + timedelta(days=1)) if notion_end != None else str(notion_end)
        else: # if the exact end time is also included
            end = str(notion_end).replace(" ", "T")

        if start != 'None':
            if 'T' in start:
                start+='-06:00'
            else:
                start+='T19:00:00-06:00'
        if end != 'None':
            if 'T' in end:
                end+='-06:00'
            else:
                end+='T20:00:00-06:00'
        return [start, end]

    #properties: 'planners', 'event_type', 'meeting_link', 'description', 'date', 'name'
    #creates list of formatted data
    events = []
    for row in all_rows:
        events.append(
            {
                'event_type': row.event_type,
                'id': row.id.replace("-", "1"), #can't use dashes in gcal identifier so must replace
                'name': row.name,
                'start_end': dates_for_gcal(row.date),
                'description': row.description,
                'meeting_link': row.meeting_link
            }
        )
    return events

def format_notion_event_for_gcal(notion_event):
    event = {
        "end": {
            "timeZone": timezone
        },
        "start": {
            "timeZone": timezone
        },
        "description": '- '.join(notion_event['event_type']) + "- " +notion_event['description'],
        "summary": notion_event['name'],
        "id": notion_event['id']
    }
    start = notion_event['start_end'][0]
    end = notion_event['start_end'][1]
    if ("T" in start) and ("T" in end):
        # case 1: specified start date & time and end date & time (ideal case)
        event['end']['dateTime'] = end
        event['start']['dateTime'] = start
    elif "T" in start or end == 'None':
        # case 2: specified start date & time and no end date & time (reminder)
        event['end']['dateTime'] = start
        event['start']['dateTime'] = start
    elif end == "None":
    #case 3: specified start date but no time (& no end)
        event['end']['date'] = start
        event['start']['date'] = start
    else:
        #case 4: specified start and end dates (no times)
        event['end']['date'] = end
        event['start']['date'] = start
    #add meeting link if exists
    if notion_event['meeting_link']:
        event['location'] = notion_event['meeting_link']
    return event

if __name__ == '__main__':
    main()