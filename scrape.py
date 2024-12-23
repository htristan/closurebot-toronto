import requests
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key
from shapely.geometry import Point, LineString
from decimal import Decimal
from discord_webhook import DiscordWebhook, DiscordEmbed
import os
from datetime import datetime, timedelta, date
import calendar
from pytz import timezone
import random
import logging

# Load config
with open('config.json', 'r') as f:
    config = json.load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # Logs to the console
    ]
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK']
AWS_ACCESS_KEY_ID = os.environ.get('AWS_DB_KEY', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_DB_SECRET_ACCESS_KEY', None) 

# (Optional) Keep a Discord username/avatar
discordUsername = "Toronto511"
discordAvatarURL = "https://www.toronto.ca/wp-content/uploads/2018/02/964b-toronto-logo.png"

# Keep filter keywords logic but do NOT filter out events in practice
FILTER_KEYWORDS = config.get('filter_keywords', [])

try:
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name='us-east-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
except Exception as e:
    print("AWS credentials are not properly configured. Ensure IAM role or environment variables are set.")
    raise

table = dynamodb.Table(config['db_name'])

# Current UTC timestamp
utc_timestamp = calendar.timegm(datetime.utcnow().timetuple())


def float_to_decimal(data):
    """Recursively convert all float values to Decimal."""
    if isinstance(data, float):
        return Decimal(str(data))
    elif isinstance(data, dict):
        return {key: float_to_decimal(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [float_to_decimal(item) for item in data]
    return data


def unix_ms_to_local_str(unix_ms):
    """
    Convert Unix timestamp in milliseconds to a local time string using config['timezone'].
    """
    if not unix_ms:
        return "Unknown"
    try:
        unix_s = int(unix_ms) / 1000
        dt_utc = datetime.utcfromtimestamp(unix_s)
        local_tz = timezone(config['timezone'])
        dt_local = dt_utc.replace(tzinfo=timezone('UTC')).astimezone(local_tz)
        return dt_local.strftime('%Y-%b-%d %I:%M %p')
    except:
        return "Unknown"


def parse_geography(item):
    """
    Parse 'geoPolyline' or lat/lng from the item to create a Shapely Point.
    If 'geoPolyline' has multiple coordinates, return its midpoint as a Point.
    """
    geo_poly = item.get("geoPolyline")
    lat_str = item.get("latitude")
    lon_str = item.get("longitude")

    # If we have a geoPolyline
    if geo_poly:
        coords_str = geo_poly.strip()
        coords_str = coords_str.replace("[", "").replace("]", "")
        parts = coords_str.split(",")
        coordinate_pairs = []

        # Parse the coordinates into pairs
        for i in range(0, len(parts), 2):
            lon_val = float(parts[i])
            lat_val = float(parts[i + 1])
            coordinate_pairs.append((lon_val, lat_val))

        # If only one pair, return it as a Point
        if len(coordinate_pairs) == 1:
            return Point(coordinate_pairs[0][0], coordinate_pairs[0][1])

        # If multiple pairs, calculate the midpoint of the LineString
        line = LineString(coordinate_pairs)
        midpoint = line.interpolate(0.5, normalized=True)  # Normalized=True gives a true midpoint
        return Point(midpoint.x, midpoint.y)

    # Fallback to lat/long if geoPolyline is empty or not present
    if lat_str and lon_str:
        return Point(float(lon_str), float(lat_str))  # (x=lon, y=lat)

    # If neither is present, return None
    return None


def contains_keywords(description, keywords=FILTER_KEYWORDS):
    """
    Return True if the description contains any of the given keywords.
    For now, we won't use the result to skip, but let's keep the function.
    """
    desc_lower = description.lower()
    for keyword in keywords:
        if keyword.lower() in desc_lower:
            return True
    return False


def build_weekly_schedule_field(event):
    """
    Collect scheduleMonday, scheduleTuesday, etc., into a single multiline string.
    Include 'workPeriod' and 'scheduleEveryday' if they exist.
    """
    schedule_lines = []

    # Include workPeriod if it exists
    work_period = event.get("workPeriod")
    if work_period:
        schedule_lines.append(f"Work Period: {work_period}")

    # Check for scheduleEveryday
    schedule_everyday = event.get("scheduleEveryday")
    if schedule_everyday:
        # Display the everyday schedule explicitly
        schedule_lines.append(f"Every Day: {schedule_everyday}")
    else:
        # Otherwise, collect individual day schedules
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        for day in days:
            field_name = f"schedule{day}"
            if event.get(field_name):
                # e.g., "Mon: 09:30-15:30"
                schedule_lines.append(f"{day[:3]}: {event[field_name]}")

    # Return a formatted string or None if no schedule data exists
    if schedule_lines:
        return "\n".join(schedule_lines)
    return None

# Send with rate limit handling
def send_webhook_with_retry(webhook, retries=1):
    """
    Executes the webhook with retry logic for rate-limiting (429) responses, capped at a single retry.
    Caps sleep duration at 2 seconds.
    """
    response = webhook.execute()
    if response.status_code in [200, 204]:
        logger.debug("Webhook executed successfully")
        return response
    elif response.status_code == 429:  # Rate limit
        if retries <= 0:
            logger.error("Rate limit exceeded and maximum retries reached. Aborting.")
            return response  # Exit gracefully after the last retry

        try:
            # Use retry_after directly (assume it's in seconds) and cap at 2 seconds
            retry_after = float(response.json().get("retry_after", 1))
            retry_after = min(retry_after, 2.0)  # Cap at 2 seconds
            logger.debug(f"Parsed retry_after: {retry_after:.2f} seconds")
        except (ValueError, TypeError, AttributeError) as e:
            logger.error(f"Failed to parse retry_after value: {e}")
            retry_after = 1.0  # Default to 1 second if parsing fails

        logger.warning(f"Rate limited. Retrying after {retry_after:.2f} seconds... (Retries left: {retries - 1})")
        time.sleep(retry_after + 0.1)  # Add a small buffer
        return send_webhook_with_retry(webhook, retries=retries - 1)  # Retry once
    else:
        logger.error(
            f"Webhook execution failed with status {response.status_code}: {response.content.decode('utf-8')}"
        )
        return response

def post_to_discord(event, post_type, point=None):
    """
    Send an event to a single Discord thread for Toronto with enhanced formatting.
    """

    post_types = {
        "closure":  {"title": "New Closure Detected", "color": 15548997},
        "update":   {"title": "Closure Updated",      "color": 16753920},
        "archived": {"title": "Closure Cleared",      "color": 52224},
    }
    if post_type not in post_types:
        raise ValueError(f"Unknown post type: {post_type}")

    embed = DiscordEmbed(
        title=post_types[post_type]["title"],
        color=post_types[post_type]["color"]
    )

    # Fields
    # 1) Event Type & "Maximum Impact"
    event_type = event.get("type", "UNKNOWN")  # e.g., "CONSTRUCTION" or "ROAD_CLOSED"
    max_impact = event.get("maxImpact", "Unknown")  # e.g., "Low", "Medium", "High"
    embed.add_embed_field(name="Event Type", value=event_type)
    embed.add_embed_field(name="Maximum Impact", value=max_impact)

    # 2) Start & End Times
    start_time = unix_ms_to_local_str(event.get("startTime"))
    end_time = unix_ms_to_local_str(event.get("endTime"))
    embed.add_embed_field(name="Start Time", value=start_time)
    embed.add_embed_field(name="End Time", value=end_time)

    # 3) Road, Name, From/To
    road_name = event.get("road", "Unknown")
    closure_name = event.get("name", "")
    from_road = event.get("fromRoad", "")
    to_road = event.get("toRoad", "")
    road_display = f"{road_name} - {closure_name}" if closure_name else road_name
    embed.add_embed_field(name="Affected Road", value=road_display)

    if from_road and to_road:
        embed.add_embed_field(name="From/To", value=f"{from_road} -> {to_road}")

    # 4) District (properly capitalized)
    district = event.get("district") or "Unknown District"  # Fallback to "Unknown District" if None
    embed.add_embed_field(name="District", value=district.title())

    # 5) Directions Affected
    directions_affected = event.get("directionsAffected", "Unknown")
    if directions_affected:
        embed.add_embed_field(name="Directions Affected", value=directions_affected)

    # 6) Work Event Type
    work_event_type = event.get("workEventType", None)
    if work_event_type:
        embed.add_embed_field(name="Work Type", value=work_event_type)

    # 7) Description
    embed.add_embed_field(name="Description", value=event.get("description", "No description provided"), inline=False)

    # 8) Created & Updated
    created_time = unix_ms_to_local_str(event.get("createdTime"))
    updated_time = unix_ms_to_local_str(event.get("lastUpdated"))
    embed.add_embed_field(name="Created", value=created_time)
    embed.add_embed_field(name="Last Updated", value=updated_time)

    # 9) Weekly Schedule
    schedule_field = build_weekly_schedule_field(event)
    if schedule_field:
        embed.add_embed_field(name="Weekly Schedule", value=schedule_field, inline=False)

    # 10) Waze Map Links
    if point:
        lat = point.y
        lon = point.x
        wme_url = f"https://www.waze.com/en-GB/editor?env=usa&lon={lon}&lat={lat}&zoomLevel=17"
        livemap_url = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={lat},{lon}&overlay=false&zoom=17"
        embed.add_embed_field(name="Map Links", value=f"[WME]({wme_url}) | [Livemap]({livemap_url})", inline=False)

    # 11) Footer / Timestamp
    embed.set_footer(text=config['license_notice'])
    if post_type in ["closure", "update"]:
        embed.set_timestamp(datetime.utcnow())
    elif post_type == "archived":
        last_touched = event.get("lastTouched")
        if last_touched:
            embed.set_timestamp(datetime.utcfromtimestamp(last_touched))
        else:
            embed.set_timestamp(datetime.utcnow())

    # Send
    webhook = DiscordWebhook(
        url=DISCORD_WEBHOOK_URL,
        username=discordUsername,
        avatar_url=discordAvatarURL
    )
    webhook.add_embed(embed)
    send_webhook_with_retry(webhook)


def fetch_all_events():
    """
    Fetch from Toronto v3 endpoint. Return in {"events": <list>} shape for consistency.
    """
    base_url = "https://secure.toronto.ca/opendata/cart/road_restrictions/v3?format=json"
    response = requests.get(base_url)
    if not response.ok:
        raise Exception(f"Error connecting to Toronto Road Restrictions API: {response.status_code}")

    data = response.json()
    # data["Closure"] is the list of closures
    closure_list = data.get("Closure", [])
    return {"events": closure_list}


def check_and_post_events():
    # (1) Cleanup if needed
    last_execution_day = get_last_execution_day()
    today = date.today().isoformat()
    if last_execution_day is None or last_execution_day < today:
        cleanup_old_events()
        update_last_execution_day()

    # (2) Fetch current closures
    data = fetch_all_events()
    # (3) Mark archived any that are missing or expired=1
    close_recent_events(data)

    eventCount = 0

    # (4) Post new or updated
    for event in data['events']:
        eventCount += 1
        is_expired = (event.get('expired', 0) == 1)
        if is_expired:
            continue  # skip, already archived or about to be archived

        # We'll treat "ACTIVE" if expired=0
        event['status'] = 'ACTIVE'

        # Keep the logic for keywords, but not skip:
        #   contains_keywords(event.get("description",""), FILTER_KEYWORDS)

        # Maximum impact filter out low and unknown
        max_impact = event.get("maxImpact", "Unknown").lower()
        if max_impact not in {'medium','high'}:
            continue

        # Build geometry
        point_or_line = parse_geography(event)
        # Attempt to find if already in DB
        dbResponse = table.query(
            KeyConditionExpression=Key('EventID').eq(event['id']),
            FilterExpression=Attr('isActive').eq(1)
        )
        if not dbResponse['Items']:
            # New event
            event['EventID'] = event['id']
            event['isActive'] = 1
            event['lastTouched'] = utc_timestamp

            # Convert float->decimal
            event = float_to_decimal(event)

            post_to_discord(event, "closure", point_or_line)
            # Store in DB
            table.put_item(Item=event)
        else:
            # Already known
            stored_item = dbResponse['Items'][0]
            stored_lastUpdated = stored_item.get('lastUpdated')
            current_lastUpdated = event.get('lastUpdated')

            event = float_to_decimal(event)

            if current_lastUpdated and stored_lastUpdated and current_lastUpdated != stored_lastUpdated:
                # Something changed => Update
                event['EventID'] = event['id']
                event['isActive'] = 1
                event['lastTouched'] = utc_timestamp

                post_to_discord(event, "update", point_or_line)
                table.put_item(Item=event)

            # Also keep lastTouched fresh if >5 minutes
            lastTouched_dt = datetime.fromtimestamp(int(stored_item.get('lastTouched', utc_timestamp)))
            now = datetime.fromtimestamp(utc_timestamp)
            diff_min = (now - lastTouched_dt).total_seconds() / 60
            diff_min += random.uniform(-2, 2)  # jitter
            if abs(diff_min) > 5:
                table.update_item(
                    Key={'EventID': event['id']},
                    UpdateExpression="SET lastTouched = :val",
                    ExpressionAttributeValues={':val': utc_timestamp}
                )

    logger.info(f"Processed {eventCount} events")


def close_recent_events(data):
    """
    Mark events as archived if they are missing from the current feed or have expired=1.
    Then post 'archived' to Discord.
    """
    active_ids = {
        e['id'] for e in data['events']
        if e.get('expired', 0) == 0
    }

    response = table.scan(FilterExpression=Attr('isActive').eq(1))
    for item in response['Items']:
        event_id = item['EventID']
        # If not in feed or is expired
        if event_id not in active_ids:
            markCompleted = True
        else:
            # Double-check if it's set to expired
            matching_item = next((x for x in data['events'] if x['id'] == event_id), None)
            if matching_item and matching_item.get('expired', 0) == 1:
                markCompleted = True
            else:
                markCompleted = False

        if markCompleted:
            # Mark isActive=0
            table.update_item(
                Key={'EventID': event_id},
                UpdateExpression="SET isActive = :val",
                ExpressionAttributeValues={':val': 0}
            )
            # Post archived
            point_or_line = parse_geography(item)
            post_to_discord(item, "archived", point_or_line)


def cleanup_old_events():
    """
    Purge events that haven't been updated in 5+ days and are inactive.
    """
    now = datetime.now()
    cutoff = now - timedelta(days=5)
    cutoff_unix = Decimal(str(cutoff.timestamp()))

    scan_params = {
        'FilterExpression': Attr('lastUpdated').lt(cutoff_unix) & Attr('isActive').eq(0)
    }
    while True:
        response = table.scan(**scan_params)
        for item in response['Items']:
            table.delete_item(Key={'EventID': item['EventID']})
        if 'LastEvaluatedKey' in response:
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break


def get_last_execution_day():
    response = table.query(KeyConditionExpression=Key('EventID').eq('LastCleanup'))
    items = response.get('Items')
    if items:
        return items[0].get('LastExecutionDay')
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
    # Record the start time
    start_time = time.time()
    check_and_post_events()
    # Calculate and log the total runtime
    total_runtime = time.time() - start_time
    logger.info(f"Total runtime: {total_runtime:.2f} seconds")

if __name__ == "__main__":
    logger.info("Running as a standalone script...")
    # Simulate the Lambda environment by passing an empty event and context
    event = {}
    context = None
    lambda_handler(event, context)
