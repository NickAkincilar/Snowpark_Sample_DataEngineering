import json
import gzip
import base64
import io
import os

print_input_event = os.environ.get('PRINT_INPUT_EVENT', 'False').lower() in ('true', '1', 't')
print_transformed_records = os.environ.get('PRINT_TRANSFORMED_RECORDS', 'False').lower() in ('true', '1', 't')

def lambda_handler(event, context):
    """
    Processes records from Kinesis Firehose. The final output for each record
    is the entire CloudWatch Log event, serialized as a single JSON object.
    """
    output_records = []

    # if print_input_event:
    #    print(f"Input event: {json.dumps(event)}")

    for record in event['records']:
        try:
            # Decode and decompress the incoming data from CloudWatch Logs
            payload_decoded = base64.b64decode(record['data'])
            compressed_payload = io.BytesIO(payload_decoded)
            uncompressed_payload = gzip.GzipFile(fileobj=compressed_payload, mode='rb')
            
            # This is the full JSON object from CloudWatch Logs
            log_data = json.loads(uncompressed_payload.read().decode('utf-8'))

            transformed_payload = ''
            if log_data.get('messageType') == 'DATA_MESSAGE':
                # STEP 1: Convert the 'log_data' Python dictionary into a JSON string.
                # This string is the desired output payload.
                transformed_payload = json.dumps(log_data)

            if not transformed_payload:
                output_records.append({
                    'recordId': record['recordId'],
                    'result': 'Dropped',
                    'data': record['data']
                })
                continue

            # STEP 2: Base64-encode the JSON string, as required by Firehose.
            transformed_data_encoded = base64.b64encode(transformed_payload.encode('utf-8')).decode('utf-8')

            # STEP 3: Place the encoded JSON into the 'data' field of the final record object.
            output_records.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': transformed_data_encoded
            })

        except Exception as e:
            print(f"Error processing record {record.get('recordId', 'N/A')}: {e}")
            output_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            })
            continue

    # if print_transformed_records:
    #    print(f"Transformed records: {json.dumps(output_records)}")

    # print(f"Successfully processed {len(output_records)} records.")

    # STEP 4: Return the final object that Firehose expects.
    return {'records': output_records}
