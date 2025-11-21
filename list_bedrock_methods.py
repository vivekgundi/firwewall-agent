import boto3

def list_bedrock_methods():
    client = boto3.client("bedrock", region_name="us-east-1")
    methods = dir(client)
    print("Available methods in Bedrock client:")
    for method in methods:
        print(method)

if __name__ == "__main__":
    list_bedrock_methods()

