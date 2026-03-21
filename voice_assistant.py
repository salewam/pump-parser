#!/usr/bin/env python3
"""ONIS Voice Pump Assistant — web interface with voice chat.

Stack: FastAPI + whisper + DeepSeek + edge-tts
Port: 5050
"""

import asyncio
import json
import logging
import os
import tempfile
import time
from pathlib import Path

import edge_tts
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === CONFIG ===
DEEPSEEK_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-90a962f26885421f84fd279e73a78572")
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
PUMP_BASE_DIR = "/root/pump_base"
TTS_VOICE = "ru-RU-DmitryNeural"  # Male Russian voice
WHISPER_MODEL = "small"
PORT = 5050

# === LOAD PUMP DATABASE ===
_pump_db = []
_pump_summary = ""

def load_pumps():
    global _pump_db, _pump_summary
    for f in sorted(os.listdir(PUMP_BASE_DIR)):
        if not f.endswith(".json"):
            continue
        series = f.replace("_BASE.json", "")
        try:
            data = json.load(open(f"{PUMP_BASE_DIR}/{f}"))
            models = data if isinstance(data, list) else data.get("models", data.get("pumps", []))
            for m in models:
                m["_series"] = series
            _pump_db.extend(models)
        except Exception as e:
            logger.warning(f"Failed to load {f}: {e}")
    
    # Build summary for LLM context
    series_counts = {}
    for p in _pump_db:
        s = p.get("_series", "?")
        series_counts[s] = series_counts.get(s, 0) + 1
    
    _pump_summary = f"База ONIS MV: {len(_pump_db)} моделей насосов.\nСерии: "
    _pump_summary += ", ".join(f"{k} ({v} шт)" for k, v in sorted(series_counts.items()))
    logger.info(f"Loaded {len(_pump_db)} pumps from {len(series_counts)} series")

load_pumps()

# === WHISPER ===
_whisper_model = None

def get_whisper():
    global _whisper_model
    if _whisper_model is None:
        from faster_whisper import WhisperModel
        _whisper_model = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")
        logger.info(f"Whisper {WHISPER_MODEL} loaded")
    return _whisper_model

def transcribe(audio_path: str) -> str:
    model = get_whisper()
    segments, _ = model.transcribe(audio_path, language="ru")
    return " ".join(s.text for s in segments).strip()

# === DEEPSEEK ===
import requests as req

SYSTEM_PROMPT = f"""Ты — голосовой ИИ-помощник компании ONIS MV, эксперт по насосному оборудованию.

{_pump_summary}

Ты помогаешь подобрать насос по параметрам (расход, напор, среда, температура), объясняешь характеристики, сравниваешь модели и даёшь рекомендации.

Правила:
- Отвечай кратко и по делу (2-5 предложений), это голосовой интерфейс
- Если спрашивают конкретную модель — дай ключевые параметры
- Если нужно подобрать — задай уточняющие вопросы (расход, напор, среда)
- Говори на русском, профессионально но дружелюбно
- Если не знаешь точного ответа — скажи что уточнишь у инженеров"""

_chat_history = {}

def chat_deepseek(user_msg: str, session_id: str = "default") -> str:
    if session_id not in _chat_history:
        _chat_history[session_id] = []
    
    history = _chat_history[session_id]
    history.append({"role": "user", "content": user_msg})
    
    # Keep last 10 messages
    if len(history) > 20:
        history = history[-20:]
        _chat_history[session_id] = history
    
    # Search pump DB for relevant info
    context = search_pumps(user_msg)
    
    messages = [{"role": "system", "content": SYSTEM_PROMPT + context}]
    messages.extend(history)
    
    try:
        r = req.post(DEEPSEEK_URL, json={
            "model": "deepseek-chat",
            "messages": messages,
            "max_tokens": 500,
            "temperature": 0.3,
        }, headers={"Authorization": f"Bearer {DEEPSEEK_KEY}"}, timeout=30)
        
        data = r.json()
        reply = data["choices"][0]["message"]["content"]
        history.append({"role": "assistant", "content": reply})
        return reply
    except Exception as e:
        logger.error(f"DeepSeek error: {e}")
        return "Извините, произошла ошибка. Попробуйте ещё раз."

def search_pumps(query: str) -> str:
    """Simple keyword search in pump database."""
    query_lower = query.lower()
    matches = []
    
    for p in _pump_db:
        name = str(p.get("model", p.get("name", ""))).lower()
        series = p.get("_series", "").lower()
        
        # Check if any word from query matches model name or series
        if any(w in name or w in series for w in query_lower.split() if len(w) > 2):
            matches.append(p)
    
    if not matches:
        return ""
    
    # Limit to top 5 matches
    matches = matches[:5]
    context = "\n\nНайденные модели по запросу:\n"
    for m in matches:
        context += json.dumps(m, ensure_ascii=False, default=str)[:300] + "\n"
    return context

# === TTS ===
async def text_to_speech(text: str) -> str:
    """Convert text to speech using edge-tts. Returns path to mp3."""
    tmp = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False, dir="/tmp")
    tmp.close()
    communicate = edge_tts.Communicate(text, TTS_VOICE)
    await communicate.save(tmp.name)
    return tmp.name

# === FASTAPI ===
app = FastAPI(title="ONIS Voice Assistant")

@app.get("/", response_class=HTMLResponse)
async def index():
    return _HTML

@app.post("/api/voice")
async def api_voice(audio: UploadFile = File(...)):
    """Voice input → transcribe → LLM → TTS → voice output."""
    t0 = time.time()
    
    # Save uploaded audio
    tmp_in = tempfile.NamedTemporaryFile(suffix=".webm", delete=False)
    tmp_in.write(await audio.read())
    tmp_in.close()
    
    # Convert to wav using ffmpeg
    wav_path = tmp_in.name.replace(".webm", ".wav")
    os.system(f"ffmpeg -y -i {tmp_in.name} -ar 16000 -ac 1 {wav_path} 2>/dev/null")
    
    # Transcribe
    text = transcribe(wav_path)
    logger.info(f"Transcribed: {text}")
    
    if not text.strip():
        os.unlink(tmp_in.name)
        os.unlink(wav_path)
        return JSONResponse({"error": "Не удалось распознать речь"})
    
    # Chat
    reply = chat_deepseek(text)
    logger.info(f"Reply: {reply[:100]}")
    
    # TTS
    audio_path = await text_to_speech(reply)
    
    # Cleanup
    os.unlink(tmp_in.name)
    os.unlink(wav_path)
    
    elapsed = time.time() - t0
    logger.info(f"Total: {elapsed:.1f}s")
    
    return JSONResponse({
        "user_text": text,
        "reply_text": reply,
        "audio_url": f"/voice/api/audio/{os.path.basename(audio_path)}",
        "elapsed": round(elapsed, 1)
    })

@app.post("/api/text")
async def api_text(request: Request):
    """Text input → LLM → TTS → voice output."""
    body = await request.json()
    text = body.get("text", "").strip()
    if not text:
        return JSONResponse({"error": "Empty text"})
    
    reply = chat_deepseek(text)
    audio_path = await text_to_speech(reply)
    
    return JSONResponse({
        "user_text": text,
        "reply_text": reply,
        "audio_url": f"/voice/api/audio/{os.path.basename(audio_path)}"
    })

@app.get("/api/audio/{filename}")
async def api_audio(filename: str):
    path = f"/tmp/{filename}"
    if os.path.exists(path):
        return FileResponse(path, media_type="audio/mpeg")
    return JSONResponse({"error": "not found"}, status_code=404)

# === HTML ===
_HTML = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ONIS Voice Assistant</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'SF Pro','Helvetica Neue',Arial,sans-serif; background:#0A0E1A; color:#E2E8F0; min-height:100vh; display:flex; flex-direction:column; }
.header { padding:16px 20px; border-bottom:1px solid rgba(255,255,255,0.06); display:flex; align-items:center; gap:12px; }
.header h1 { font-size:18px; font-weight:700; }
.header .badge { font-size:10px; background:rgba(16,185,129,0.15); color:#10B981; padding:2px 8px; border-radius:6px; }
.chat { flex:1; overflow-y:auto; padding:16px 20px; display:flex; flex-direction:column; gap:12px; }
.msg { max-width:85%; padding:12px 16px; border-radius:16px; font-size:14px; line-height:1.5; animation:fadeIn .3s; }
.msg.user { align-self:flex-end; background:rgba(99,102,241,0.2); border:1px solid rgba(99,102,241,0.3); border-bottom-right-radius:4px; }
.msg.bot { align-self:flex-start; background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08); border-bottom-left-radius:4px; }
.msg .meta { font-size:10px; color:#475569; margin-top:4px; }
.controls { padding:12px 20px; border-top:1px solid rgba(255,255,255,0.06); display:flex; gap:8px; align-items:center; }
.text-input { flex:1; padding:12px 16px; border-radius:12px; border:1px solid rgba(255,255,255,0.08); background:rgba(255,255,255,0.03); color:#E2E8F0; font-size:14px; font-family:inherit; outline:none; }
.text-input:focus { border-color:rgba(99,102,241,0.5); }
.btn-mic { width:48px; height:48px; border-radius:50%; border:2px solid rgba(239,68,68,0.4); background:rgba(239,68,68,0.1); color:#F87171; font-size:20px; cursor:pointer; display:flex; align-items:center; justify-content:center; transition:all .2s; flex-shrink:0; }
.btn-mic:hover { background:rgba(239,68,68,0.2); }
.btn-mic.recording { background:rgba(239,68,68,0.4); border-color:#EF4444; animation:pulse 1s infinite; }
.btn-send { padding:12px 20px; border-radius:12px; border:1px solid rgba(99,102,241,0.3); background:rgba(99,102,241,0.15); color:#818CF8; font-size:14px; cursor:pointer; font-family:inherit; }
.btn-send:hover { background:rgba(99,102,241,0.25); }
.thinking { display:flex; gap:4px; padding:8px 16px; }
.thinking span { width:8px; height:8px; background:#475569; border-radius:50%; animation:bounce .6s infinite; }
.thinking span:nth-child(2) { animation-delay:.15s; }
.thinking span:nth-child(3) { animation-delay:.3s; }
@keyframes fadeIn { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }
@keyframes bounce { 0%,100%{transform:translateY(0)} 50%{transform:translateY(-6px)} }
</style>
</head>
<body>

<div class="header">
  <div style="font-size:28px">🔧</div>
  <div>
    <h1>ONIS Voice Assistant</h1>
    <div style="font-size:11px;color:#475569">Голосовой помощник по насосному оборудованию</div>
  </div>
  <div class="badge">""" + str(len(_pump_db)) + """ моделей</div>
</div>

<div class="chat" id="chat">
  <div class="msg bot">Здравствуйте! Я голосовой помощник ONIS MV. Помогу подобрать насос, расскажу о характеристиках или сравню модели. Говорите или пишите!</div>
</div>

<div class="controls">
  <input class="text-input" id="text-input" placeholder="Напишите или нажмите микрофон..." onkeydown="if(event.key==='Enter')sendText()">
  <button class="btn-send" onclick="sendText()">→</button>
  <button class="btn-mic" id="mic-btn" onclick="toggleMic()">🎤</button>
</div>

<script>
const chat=document.getElementById('chat');
const _base=window.location.pathname.replace(/\/$/,'');
let mediaRecorder=null, audioChunks=[], recording=false;

function addMsg(text, type, extra=''){
  const d=document.createElement('div');
  d.className='msg '+type;
  d.innerHTML=text.replace(/\\n/g,'<br>')+(extra?'<div class="meta">'+extra+'</div>':'');
  chat.appendChild(d);
  chat.scrollTop=chat.scrollHeight;
  return d;
}

function showThinking(){
  const d=document.createElement('div');
  d.className='thinking';d.id='thinking';
  d.innerHTML='<span></span><span></span><span></span>';
  chat.appendChild(d);
  chat.scrollTop=chat.scrollHeight;
}
function hideThinking(){const t=document.getElementById('thinking');if(t)t.remove();}

async function sendText(){
  const inp=document.getElementById('text-input');
  const text=inp.value.trim();if(!text)return;
  inp.value='';
  addMsg(text,'user');
  showThinking();
  try{
    const r=await fetch(_base+'/api/text',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text})});
    const d=await r.json();
    hideThinking();
    if(d.error){addMsg(d.error,'bot');return;}
    addMsg(d.reply_text,'bot');
    if(d.audio_url){const a=new Audio(d.audio_url);a.play();}
  }catch(e){hideThinking();addMsg('Ошибка соединения','bot');}
}

async function toggleMic(){
  const btn=document.getElementById('mic-btn');
  if(!recording){
    try{
      const stream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true}});
      // Try webm first, fallback to default
      let options={};
      if(MediaRecorder.isTypeSupported('audio/webm;codecs=opus')){
        options={mimeType:'audio/webm;codecs=opus'};
      }else if(MediaRecorder.isTypeSupported('audio/webm')){
        options={mimeType:'audio/webm'};
      }else if(MediaRecorder.isTypeSupported('audio/mp4')){
        options={mimeType:'audio/mp4'};
      }
      mediaRecorder=new MediaRecorder(stream,options);
      audioChunks=[];
      mediaRecorder.ondataavailable=e=>{if(e.data.size>0)audioChunks.push(e.data);};
      mediaRecorder.onstop=async()=>{
        stream.getTracks().forEach(t=>t.stop());
        const blob=new Blob(audioChunks,{type:mediaRecorder.mimeType||'audio/webm'});
        await sendVoice(blob);
      };
      mediaRecorder.start(100);
      recording=true;
      btn.classList.add('recording');
      btn.textContent='⏹';
    }catch(e){addMsg('Нет доступа к микрофону: '+e.message,'bot');}
  }else{
    mediaRecorder.stop();
    recording=false;
    btn.classList.remove('recording');
    btn.textContent='🎤';
  }
}

async function sendVoice(blob){
  addMsg('🎤 Записано, обрабатываю...','user');
  showThinking();
  const fd=new FormData();
  fd.append('audio',blob,'voice.webm');
  try{
    const r=await fetch(_base+'/api/voice',{method:'POST',body:fd});
    if(!r.ok){hideThinking();addMsg('Ошибка сервера: '+r.status,'bot');return;}
    const d=await r.json();
    hideThinking();
    if(d.error){addMsg(d.error,'bot');return;}
    const msgs=chat.querySelectorAll('.msg.user');
    const last=msgs[msgs.length-1];
    if(last)last.innerHTML=d.user_text+'<div class="meta">🎤 распознано</div>';
    const botMsg=addMsg(d.reply_text,'bot',d.elapsed?d.elapsed+'с':'');
    if(d.audio_url){
      const a=new Audio(d.audio_url);
      a.onerror=()=>console.log('Audio play error');
      a.play().catch(e=>console.log('Autoplay blocked'));
    }
  }catch(e){hideThinking();addMsg('Ошибка: '+e.message,'bot');}
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
