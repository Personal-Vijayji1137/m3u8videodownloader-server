from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from typing import List, Dict
from pydantic import BaseModel
from urllib.parse import urljoin
import asyncio
import aiohttp
import subprocess
import json
import websockets
import jwt
import boto3
import shutil
import os
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
app = FastAPI()
class ImageRequest(BaseModel):
    token: str


rooms: Dict[str, List[WebSocket]] = {}

@app.websocket("/ws/{room_name}")
async def websocket_endpoint(websocket: WebSocket, room_name: str):
    await websocket.accept()
    if room_name not in rooms:
        rooms[room_name] = []
    rooms[room_name].append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            for connection in rooms[room_name]:
                if connection != websocket:
                    await connection.send_text(data)
    except WebSocketDisconnect:
        rooms[room_name].remove(websocket)
        if not rooms[room_name]:
            del rooms[room_name]





def decode_jwt_token(token: str) -> Dict:
    try:
        decoded_data = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
        return decoded_data
    except jwt.ExpiredSignatureError:
        return False
    except jwt.InvalidTokenError:
        return False




def generate_presigned_url(s3_client, bucket_name, s3_key, expiration=86400):
    try:
        url = s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': s3_key},ExpiresIn=expiration)
        return url
    except Exception as e:
        return None
    
async def download_segment(session, url, output_path):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            with open(output_path, "wb") as f:
                while chunk := await response.content.read(8192):
                    f.write(chunk)
        return output_path
    except Exception as e:
        return None

def convert_ts_to_mp4(ts_file, output_mp4_path):
    try:
        command = ["ffmpeg", "-y", "-i", ts_file, "-c", "copy", output_mp4_path]
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        raise

async def upload_to_s3(s3_client, file_path, bucket_name, s3_key):
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
    except Exception as e:
        raise

@app.get("/")
async def download_home_page():
     return {"Message_From_My_Side": "Tu Thoda Sa Bhan Ka Loda hai kaaa ..."}

@app.post("/")
async def download_and_upload_m3u8(request: ImageRequest):
    decoded_data = decode_jwt_token(request.token)
    image_url = decoded_data["image_url"]
    bucket_name = decoded_data["bucket_name"]
    s3_key = decoded_data["s3_key"]
    access_id = decoded_data["access_id"]
    secret_access_key = decoded_data["secret_access_key"]
    region_name = decoded_data["region_name"]

    segment_folder = '/app/segment_folder'
    output_ts_path = os.path.join(segment_folder, "combined_segments.ts")
    output_mp4_path = os.path.join(segment_folder, "final_video.mp4")

    # Clean up any existing files
    if not os.path.exists(segment_folder):
        os.makedirs(segment_folder)
    room_name = request.token.split(".")[1]
    websocket_url = f"wss://iplustsolution-m3u8videodownloader.hf.space/ws/{room_name}"
    s3_client = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=secret_access_key, region_name=region_name)
    async with websockets.connect(websocket_url) as websocket:
        async with aiohttp.ClientSession() as session:
            try:
                # Step 1: Download the m3u8 file
                await websocket.send(json.dumps({"message":"We Recive your request ...","progress":10}))
                async with session.get(image_url) as resp:
                    resp.raise_for_status()
                    m3u8_content = await resp.text()

                # Step 2: Extract segment URLs from m3u8 content
                await websocket.send(json.dumps({"message":"Reading M3U8 URL ...","progress":20}))
                base_url = image_url.rsplit("/", 1)[0] + "/"
                segment_urls = [urljoin(base_url, line) for line in m3u8_content.splitlines() if line and not line.startswith("#")]
                download_tasks = []
                for i, segment_url in enumerate(segment_urls):
                    output_path = os.path.join(segment_folder, f"segment_{i}.ts")
                    task = download_segment(session, segment_url, output_path)
                    download_tasks.append(task)

                # Step 3: Download all segments in parallel
                await websocket.send(json.dumps({"message":"Downloading Your File on server ...","progress":30}))
                downloaded_files = await asyncio.gather(*download_tasks)
                downloaded_files = [f for f in downloaded_files if f]

                # Step 4: Concatenate all downloaded segments into a single .ts file
                await websocket.send(json.dumps({"message":"Concatenate all downloaded segments into a single file","progress":40}))
                with open(output_ts_path, "wb") as combined:
                    for file_path in downloaded_files:
                        with open(file_path, "rb") as segment_file:
                            shutil.copyfileobj(segment_file, combined)

                # Step 5: Convert the combined .ts file to .mp4 format
                await websocket.send(json.dumps({"message":"Convert the file to .mp4 format","progress":50}))
                convert_ts_to_mp4(output_ts_path, output_mp4_path)

                await websocket.send(json.dumps({"message":"Convert the file to .mp4 format","progress":60}))
                # Step 6: Upload the .mp4 file to S3
                await upload_to_s3(s3_client, output_mp4_path, bucket_name, s3_key)
                file_size_in_bytes = os.path.getsize(output_mp4_path)
                file_size_in_mb = file_size_in_bytes / (1024 * 1024)
                presigned_url = generate_presigned_url(s3_client, bucket_name, s3_key)

                # Clean up temporary files
                await websocket.send(json.dumps({"message":"Convert the file to .mp4 format","progress":70}))
                for file_path in downloaded_files:
                    os.remove(file_path)
                os.remove(output_ts_path)
                os.remove(output_mp4_path)
                await websocket.send(json.dumps({"message":"Done","progress":100}))
                if presigned_url:
                    return {"status": "File uploaded successfully", "presigned_url": presigned_url,"file_size_mb": round(file_size_in_mb, 2)}
                else:
                    return {"status": "File uploaded successfully, but failed to generate presigned URL"}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to process m3u8 file: {e}")