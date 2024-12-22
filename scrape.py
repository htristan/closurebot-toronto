import requests
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key
from shapely.geometry import Point, Polygon, LineString
from decimal import Decimal
from discord_webhook import DiscordWebhook, DiscordEmbed
import os
from datetime import datetime, timedelta, date
import calendar
from pytz import timezone
import random

# Load the configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK']
AWS_ACCESS_KEY_ID = os.environ.get('AWS_DB_KEY', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_DB_SECRET_ACCESS_KEY', None)

discordUsername = "DriveBC"
discordAvatarURL = "https://pbs.twimg.com/profile_images/961736998745600000/Zrqm1EiB_400x400.jpg"

# Fetch filter keywords from the config file
FILTER_KEYWORDS = config.get('filter_keywords', [])

# Fallback mechanism for credentials
try:
    # Use environment variables if they exist
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name='us-east-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    else:
        # Otherwise, use IAM role permissions (default behavior of boto3)
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
except (NoCredentialsError, PartialCredentialsError):
    print("AWS credentials are not properly configured. Ensure IAM role or environment variables are set.")
    raise

# Specify the name of your DynamoDB table
table = dynamodb.Table(config['db_name'])

# set the current UTC timestamp for use in a few places
utc_timestamp = calendar.timegm(datetime.utcnow().timetuple())

def float_to_decimal(data):
    """
    Recursively converts all float values in the input (dict, list, or scalar) to Decimal,
    as DynamoDB does not support float.
    """
    if isinstance(data, float):
        return Decimal(str(data))  # Convert float to Decimal
    elif isinstance(data, dict):
        return {key: float_to_decimal(value) for key, value in data.items()}  # Recurse into dict
    elif isinstance(data, list):
        return [float_to_decimal(item) for item in data]  # Recurse into list
    else:
        return data  # Return data as-is if not float, dict, or list

def getThreadID(threadName):
    if threadName == 'LowerMainland':
        return config['Thread-LowerMainland']
    elif threadName == 'VancouverIsland':
        return config['Thread-VancouverIsland']
    else:
        return config['Thread-CatchAll'] #Other catch all thread

def unix_to_readable_with_timezone(iso_timestamp):
    """
    Converts an ISO 8601 timestamp with a timezone offset into a human-readable string.
    If no timezone is present, defaults to the configured timezone in `config['timezone']`.
    """
    try:
        # Parse the timestamp with dateutil to include timezone info
        parsed_time = datetime.fromisoformat(iso_timestamp)
    except ValueError:
        # Handle improperly formatted timestamps
        return "Invalid Timestamp"

    # Convert to local timezone if present; fallback to config['timezone']
    if parsed_time.tzinfo is not None:
        local_time = parsed_time
    else:
        # If no timezone info exists, use the configured timezone as fallback
        fallback_tz = timezone(config['timezone'])
        local_time = parsed_time.replace(tzinfo=timezone('UTC')).astimezone(fallback_tz)

    return local_time.strftime('%Y-%b-%d %I:%M %p')

def parse_geography(geography):
    """
    Parses the geography field to extract a representative Point.
    Handles Point and LineString geometries.
    """
    if geography['type'] == 'Point':
        # Return the Point directly
        coordinates = geography['coordinates']
        return Point(coordinates[0], coordinates[1])
    elif geography['type'] == 'LineString':
        # Handle LineString: pick the midpoint as the representative point
        line = LineString(geography['coordinates'])
        return line.interpolate(0.5, normalized=True)  # Midpoint
    else:
        raise ValueError(f"Unsupported geography type: {geography['type']}")

def check_which_polygon(areaName):
    # Function to see which polygon a point is in, and returns the text. Returns "Other" if unknown.
    if areaName == 'Lower Mainland District':
        return 'LowerMainland'
    elif areaName == 'Vancouver Island District':
        return 'VancouverIsland'
    else:
        return 'Other'

def parse_time_with_fallback(iso_time_str, fallback_tz):
    """
    Parses an ISO 8601 timestamp, applying a fallback timezone if none exists in the input.
    """
    # Check if the string includes timezone information
    if iso_time_str.endswith("Z"):  # UTC timezone indicated by 'Z'
        dt = datetime.fromisoformat(iso_time_str.replace("Z", "+00:00"))
    elif "+" in iso_time_str or "-" in iso_time_str[10:]:  # Already has a timezone offset
        dt = datetime.fromisoformat(iso_time_str)
    else:  # No timezone information, apply the fallback timezone
        naive_dt = datetime.fromisoformat(iso_time_str)
        dt = naive_dt.replace(tzinfo=fallback_tz)

    return dt

def contains_keywords(description, keywords=FILTER_KEYWORDS):
    """
    Check if the description contains any of the specified keywords.

    Args:
        description (str): The text to search for keywords.
        keywords (list, optional): A list of keywords to search for. Defaults to FILTER_KEYWORDS.

    Returns:
        bool: True if any keyword is found in the description; False otherwise.
    """
    description = description.lower()
    for keyword in keywords:
        if keyword.lower() in description:
            return True
    return False

def post_to_discord(event, post_type, threadName, point=None):
    """
    Posts a message to Discord based on the type of event (closure, update, or archived),
    including thread routing and geo information.
    """
    # Get the thread ID based on the thread name
    threadID = getThreadID(threadName)

    # Get the title and color based on the post type
    post_types = {
        "closure": {"title": "New Event Detected", "color": 15548997},
        "update": {"title": "Event Updated", "color": 16753920},
        "archived": {"title": "Event Cleared", "color": 52224},
    }
    if post_type not in post_types:
        raise ValueError(f"Unknown post type: {post_type}")
    title = post_types[post_type]["title"]
    color = post_types[post_type]["color"]

    # Create the embed
    embed = DiscordEmbed(title=title, color=color)

    # Add Event Type and Severity
    event_type = event.get("event_type", "UNKNOWN")
    severity = event.get("severity", "UNKNOWN")
    embed.add_embed_field(name="Event Type", value=f"{event_type.capitalize()} - {severity.capitalize()}")

    # Add Timing Information
    schedule = event.get("schedule", {})
    intervals = schedule.get("intervals", [])
    recurring_schedules = schedule.get("recurring_schedules", [])
    start_time = None

    if intervals:
        # Handle intervals if provided
        interval_start_str = intervals[0].split("/")[0]  # Extract start time from interval
        start_time = unix_to_readable_with_timezone(interval_start_str)
        embed.add_embed_field(name="Start Time", value=start_time)
    elif recurring_schedules:
        # Handle recurring schedules if provided
        recurring = recurring_schedules[0]  # Assume the first recurring schedule
        start_date = unix_to_readable_with_timezone(recurring.get("start_date", "Unknown"))
        end_date = unix_to_readable_with_timezone(recurring.get("end_date", "Unknown"))
        daily_start = recurring.get("daily_start_time", "Unknown")
        daily_end = recurring.get("daily_end_time", "Unknown")
        embed.add_embed_field(
            name="Schedule",
            value=f"{start_date} to {end_date}\nDaily: {daily_start} - {daily_end}"
        )

    # Add Road Information
    roads = event.get("roads", [])
    if roads:
        road_info = "\n".join(
            [
                f"{road['name']} ({road['direction']})"
                + (f" from {road['from']}" if "from" in road else "")
                + (f" to {road['to']}" if "to" in road else "")
                for road in roads
            ]
        )
        embed.add_embed_field(name="Affected Roads", value=road_info, inline=False)

    # Add Description
    embed.add_embed_field(name="Description", value=event.get("description", "No description provided"), inline=False)

    # Add Metadata
    created_time = unix_to_readable_with_timezone(event.get("created", "Unknown"))
    updated_time = unix_to_readable_with_timezone(event.get("updated", "Unknown"))
    embed.add_embed_field(name="Created", value=created_time)
    embed.add_embed_field(name="Last Updated", value=updated_time)

    driveBCID = event['id'].split("/")[-1]
    url511 = f"https://www.drivebc.ca/mobile/pub/events/id/{driveBCID}.html"
    # Add Geo Information and Map Links
    if point:
        latitude = point.y
        longitude = point.x
        url_wme = f"https://www.waze.com/en-GB/editor?env=usa&lon={longitude}&lat={latitude}&zoomLevel=15"
        url_livemap = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={latitude},{longitude}&overlay=false&zoom=16"
        if post_type == 'archived':
            embed.add_embed_field(name="Map Links", value=f"[WME]({url_wme}) | [Livemap]({url_livemap})", inline=False)
        else:
            embed.add_embed_field(name="Map Links", value=f"[DriveBC]({url511}) | [WME]({url_wme}) | [Livemap]({url_livemap})", inline=False)


    # Set Footer
    embed.set_footer(text=config['license_notice'])

    # Set the timestamp
    if post_type == "closure":
        # Final fallback to the current UTC time
        embed.set_timestamp(datetime.utcnow())
    elif post_type == "update":
        embed.set_timestamp(datetime.utcnow())
    elif post_type == "archived":
        last_touched = event.get("lastTouched", None)
        if last_touched:
            embed.set_timestamp(datetime.utcfromtimestamp(last_touched))
        else:
            # Fallback to the current UTC time if lastTouched is missing
            embed.set_timestamp(datetime.utcnow())

    # Send to Discord
    webhook = DiscordWebhook(
        url=DISCORD_WEBHOOK_URL,
        username=discordUsername,
        avatar_url=discordAvatarURL,
        thread_id=threadID  # Route to the correct thread
    )
    webhook.add_embed(embed)
    webhook.execute()

def fetch_all_events():
    base_url = "https://api.open511.gov.bc.ca/events"
    limit = 300  # Define the limit per API call
    severities = ["MAJOR", "MODERATE"]  # List of severities to filter
    all_events = []  # List to store all events

    for severity in severities:
        offset = 0  # Start at the beginning for each severity
        while True:
            # Make the API request with limit, offset, and severity filter
            response = requests.get(f"{base_url}?severity={severity}&limit={limit}&offset={offset}")
            
            if not response.ok:
                raise Exception(f"Error connecting to BC511 API for severity {severity}: {response.status_code} - {response.text}")
            
            # Parse the response JSON
            data = response.json()
            
            # Extract the events
            events = data.get('events', [])
            if not events:
                break  # Exit the loop if no more events are returned

            # Add the events to the list
            all_events.extend(events)

            # Increment the offset by the limit for the next batch
            offset += limit

            # Break if fewer events were returned than the limit (last page)
            if len(events) < limit:
                break

    # Return a dictionary structured like the original API response
    return {"events": all_events}

def check_and_post_events():
    #check if we need to clean old events
    last_execution_day = get_last_execution_day()
    today = date.today().isoformat()
    if last_execution_day is None or last_execution_day < today:
        # Perform cleanup of old events
        cleanup_old_events()

        # Update last execution day to current date
        update_last_execution_day()

    # Fetch Events from BC API
    data = fetch_all_events()

    #use the response to close out anything recent
    close_recent_events(data)

    # Iterate over the events
    for event in data['events']:
        if event['status'] == 'ACTIVE':
            # Parse geography
            geography = event.get('geography', {})
            if not geography:
                continue  # Skip if no geographical info is available

            # Extract a representative point
            point = parse_geography(geography)

            # Determine the area name from 'areas'
            if 'areas' in event and isinstance(event['areas'], list) and len(event['areas']) > 0:
                area_name = event['areas'][0].get('name', 'Unknown')  # Safely get the 'name' field
            else:
                area_name = None
            detectedPolygon = check_which_polygon(area_name)

            # Try to get the event with the specified ID and isActive=1 from the DynamoDB table
            dbResponse = table.query(
                KeyConditionExpression=Key('EventID').eq(event['id']),
                FilterExpression=Attr('isActive').eq(1)
            )

            # Get event description
            description = event.get("description", "")

            #If the event is not in the DynamoDB table
            if not dbResponse['Items']:
                # If the event is new, apply the keyword filter
                if not contains_keywords(description):
                    continue  # Skip if keywords are not found
                # Set the EventID key in the event data
                event['EventID'] = event['id']
                # Set the isActive attribute
                event['isActive'] = 1
                # set LastTouched
                event['lastTouched'] = utc_timestamp
                event['DetectedPolygon'] = detectedPolygon
                # Convert float values in the event to Decimal
                event = float_to_decimal(event)
                # If the event is within the specified area and has not been posted before, post it to Discord
                post_to_discord(event,'closure',detectedPolygon,point)
                # Add the event ID to the DynamoDB table
                table.put_item(Item=event)
            else:
                # We have seen this event before
                # First, let's see if it has a lastupdated time
                event = float_to_decimal(event)
                stored_event = dbResponse['Items'][0]
                lastUpdated = stored_event.get('LastUpdated')
                if lastUpdated != None:
                    # Now, see if the version we stored is different
                    if lastUpdated != stored_event['LastUpdated']:
                        # Store the most recent updated time:
                        event['EventID'] = event['id']
                        event['isActive'] = 1
                        event['lastTouched'] = utc_timestamp
                        event['DetectedPolygon'] = check_which_polygon_point(point)
                        # It's different, so we should fire an update notification
                        post_to_discord(event,'update',detectedPolygon,point)
                        table.put_item(Item=event)
                # get the lasttouched time
                lastTouched_datetime = datetime.fromtimestamp(int(dbResponse['Items'][0].get('lastTouched')))
                # store the current time now
                now = datetime.fromtimestamp(utc_timestamp)
                # Compute the difference in minutes between now and lastUpdated
                time_diff_min = (now - lastTouched_datetime).total_seconds() / 60
                # Compute the variability
                variability = random.uniform(-2, 2)  # random float between -2 and 2
                # Add variability to the time difference
                time_diff_min += variability
                # If time_diff_min > 5, then more than 5 minutes have passed (considering variability)
                if abs(time_diff_min) > 5:
                    # let's store that we just saw it to keep track of the last touch time
                    table.update_item(
                        Key={'EventID': event['id']},
                        UpdateExpression="SET lastTouched = :val",
                        ExpressionAttributeValues={':val': utc_timestamp}
                    )

def close_recent_events(data):
    # Ensure the expected structure is present
    if 'events' not in data or not isinstance(data['events'], list):
        raise KeyError("The API response does not contain a valid 'events' list.")

    # Create a set of active event IDs
    active_event_ids = {event['id'] for event in data['events']}

    # Get the list of event IDs in the table
    response = table.scan(
        FilterExpression=Attr('isActive').eq(1)
    )

    # Iterate over the items
    for item in response['Items']:
        markCompleted = False
        event_id = item['EventID']

        # Extract a representative point
        point = parse_geography(item['geography'])

        # If an item's ID is not in the set of active event IDs, mark it as closed
        if event_id not in active_event_ids:
            markCompleted = True
        else:
            # item exists, but now we need to check to see if it's no longer a full closure
            event_data = next((e for e in data['events'] if e['id'] == event_id), None)
            if event_data and event_data.get('status', '').upper() != 'ACTIVE':
                #now it's no longer a full closure - markt it as closed.
                markCompleted = True

        # process relevant completions
        if markCompleted:
            # Convert float values in the item to Decimal
            item = float_to_decimal(item)

            # Remove the isActive attribute from the item
            table.update_item(
                Key={'EventID': event_id},
                UpdateExpression="SET isActive = :val",
                ExpressionAttributeValues={':val': 0}
            )
            # Notify about closure on Discord
            if 'DetectedPolygon' in item and item['DetectedPolygon'] is not None:
                post_to_discord(item,'archived',item['DetectedPolygon'],point)
            else:
                post_to_discord(item,'archived',None,point)

def cleanup_old_events():
    # Get the current time and subtract 5 days to get the cut-off time
    now = datetime.now()
    cutoff = now - timedelta(days=5)
    # Convert the cutoff time to Unix timestamp
    cutoff_unix = Decimal(str(cutoff.timestamp()))
    # Initialize the scan parameters
    scan_params = {
        'FilterExpression': Attr('LastUpdated').lt(cutoff_unix) & Attr('isActive').eq(0)
    }
    while True:
        # Perform the scan operation
        response = table.scan(**scan_params)
        # Iterate over the matching items and delete each one
        for item in response['Items']:
            table.delete_item(
                Key={
                    'EventID': item['EventID']
                }
            )
        # If the scan returned a LastEvaluatedKey, continue the scan from where it left off
        if 'LastEvaluatedKey' in response:
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            # If no LastEvaluatedKey was returned, the scan has completed and we can break from the loop
            break

def get_last_execution_day():
    response = table.query(
        KeyConditionExpression=Key('EventID').eq('LastCleanup')
    )

    items = response.get('Items')
    if items:
        item = items[0]
        last_execution_day = item.get('LastExecutionDay')
        return last_execution_day

    return None

def update_last_execution_day():
    today = datetime.now().date().isoformat()
    table.put_item(
        Item={
            'EventID': 'LastCleanup',
            'LastExecutionDay': today
        }
    )

def lambda_handler(event, context):
    check_and_post_events()

if __name__ == "__main__":
    print("Running as a standalone script...")
    check_and_post_events()