"""Autonomous MENA energy-flow disruption warning from official public data."""
from __future__ import annotations
import json,math,statistics
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from typing import Any
import requests
from bs4 import BeautifulSoup

ROOT=Path(__file__).resolve().parents[1]; OUTPUT=ROOT/"data"/"output.json"; TIMEOUT=45; FALLBACK_HOURS=72
UA="MonarchCastleTech-MENAEnergy/2.0 (public research; github.com/MonarchCastleTech/mena-energy-flow)"
WEIGHTS={"chokepoint_flow":.35,"energy_port_flow":.25,"energy_markets":.20,"regional_sanctions":.10,"port_weather":.10}
BASE="https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"; CHOKE=f"{BASE}/Daily_Chokepoints_Data/FeatureServer/0"; PORTS=f"{BASE}/PortWatch_ports/FeatureServer/1"; PORT_FLOW=f"{BASE}/Daily_Ports_Data/FeatureServer/0"
FRED="https://fred.stlouisfed.org/graph/fredgraph.csv"; OFAC="https://ofac.treasury.gov/press-releases"; MET="https://api.met.no/weatherapi/locationforecast/2.0/compact"
CHOKES={"chokepoint1":"Suez Canal","chokepoint4":"Bab el-Mandeb Strait","chokepoint6":"Strait of Hormuz"}
ENERGY_PORTS={"port362","port743","port1090","port512","port526","port1091","port1341","port108","port1203","port988","port24","port1342"}
REGION=("iran","iraq","yemen","syria","lebanon","libya","saudi","emirates","qatar","oman","kuwait","bahrain","egypt","houthi","hezbollah")
ENERGY=("oil","petroleum","gas","lng","shipping","tanker","energy","refinery","pipeline")
ACTION={"sanction":1.5,"designat":2,"terror":2,"network":1,"smuggl":2,"shipping":1.5,"petroleum":2}
SOURCES=[
 {"name":"IMF PortWatch","role":"AIS-derived chokepoint and port calls","url":"https://portwatch.imf.org/"},
 {"name":"FRED","role":"Brent and natural-gas price series","url":"https://fred.stlouisfed.org/"},
 {"name":"U.S. Treasury OFAC","role":"Regional energy-linked sanctions","url":OFAC},
 {"name":"MET Norway Locationforecast","role":"ECMWF-based seven-day port weather","url":"https://api.met.no/weatherapi/locationforecast/2.0/documentation"},
]
def clamp(v:float,lo:float=0,hi:float=100)->float:return max(lo,min(hi,v))
def avg(v:list[float])->float:return sum(v)/len(v) if v else 0.0
def num(v:Any)->float:
 try:return float(v or 0)
 except (TypeError,ValueError):return 0.0
def pdate(v:Any)->date|None:
 try:return date.fromisoformat(str(v)[:10])
 except (TypeError,ValueError):return None
def pdt(v:Any)->datetime|None:
 try:
  d=datetime.fromisoformat(str(v).replace("Z","+00:00"));return d.replace(tzinfo=d.tzinfo or timezone.utc).astimezone(timezone.utc)
 except (TypeError,ValueError):return None
def rz(cur:float,base:list[float])->float:
 if len(base)<3:return 0
 m=statistics.median(base);mad=statistics.median(abs(x-m) for x in base)
 if mad>1e-9:return (cur-m)/(1.4826*mad)
 sd=statistics.pstdev(base);return (cur-m)/sd if sd>1e-9 else 0
def band(s:float)->str:return "BASELINE" if s<25 else "WATCH" if s<45 else "ELEVATED" if s<65 else "HIGH" if s<80 else "SEVERE"
def get(url:str,**kw:Any)->requests.Response:
 r=requests.get(url,headers={"User-Agent":UA,"Accept":"*/*"},timeout=kw.pop("timeout",TIMEOUT),**kw);r.raise_for_status();return r
def arc(layer:str,where:str,fields:str,count:int=1000,order:str|None=None)->list[dict[str,Any]]:
 p={"where":where,"outFields":fields,"returnGeometry":"false","resultRecordCount":count,"f":"json"}
 if order:p["orderByFields"]=order
 d=get(f"{layer}/query",params=p).json()
 if d.get("error"):raise RuntimeError(str(d["error"]))
 return [x.get("attributes",{}) for x in d.get("features",[])]
def pressure(vals:list[float])->tuple[float,float,str,float]:
 recent=avg(vals[:7]);base=avg(vals[7:35])
 if base<=0:return 0,0,"insufficient",0
 change=(recent/base-1)*100; raw=clamp((abs(change)-5)/35*100); reliability=min(1,math.sqrt(base/2));return raw*reliability,change,"surge" if change>=0 else "shortfall",reliability
def series(entity:dict[str,Any],layer:str,field:str)->dict[str,Any]|None:
 rows=arc(layer,f"portid='{entity['id']}'",f"date,{field}",42,"date DESC");vals=[num(r.get(field)) for r in rows if r.get(field) is not None]
 if len(vals)<21:return None
 p,c,d,r=pressure(vals);return {**entity,"recent_mean":round(avg(vals[:7]),1),"baseline_mean":round(avg(vals[7:35]),1),"change_pct":round(c,1),"direction":d,"reliability":round(r,2),"pressure":round(p,1),"latest":rows[0].get("date")}
def collect_chokes(now:datetime)->dict[str,Any]:
 with ThreadPoolExecutor(max_workers=3) as pool:rows=[x for x in pool.map(lambda kv:series({"id":kv[0],"name":kv[1]},CHOKE,"n_total"),CHOKES.items()) if x]
 score=round(clamp(.6*avg([x["pressure"] for x in rows])+.4*max([x["pressure"] for x in rows],default=0)),1);return {"key":"chokepoint_flow","score":score,"status":band(score),"weight":WEIGHTS["chokepoint_flow"],"available":True,"retained":False,"coverage":len(rows),"method":"Seven-day vessel flow versus preceding 28 days at Suez, Bab el-Mandeb and Hormuz.","evidence":sorted(rows,key=lambda x:x["pressure"],reverse=True)}
def port_directory()->list[dict[str,Any]]:
 ids=",".join(f"'{x}'" for x in ENERGY_PORTS);rows=arc(PORTS,f"portid IN ({ids})","portid,portname,country,lat,long,vessel_count_total",30,"vessel_count_total DESC")
 return [{"id":r["portid"],"name":r["portname"],"country":r.get("country"),"lat":num(r.get("lat")),"lon":num(r.get("long"))} for r in rows]
def collect_ports(ports:list[dict[str,Any]],now:datetime)->dict[str,Any]:
 with ThreadPoolExecutor(max_workers=8) as pool:rows=[x for x in pool.map(lambda p:series(p,PORT_FLOW,"portcalls"),ports) if x]
 weights=[max(1,x["baseline_mean"]) for x in rows];weighted=sum(x["pressure"]*w for x,w in zip(rows,weights))/sum(weights);leaders=sorted((x["pressure"] for x in rows),reverse=True)[:3];score=round(clamp(.55*weighted+.45*avg(leaders)),1)
 return {"key":"energy_port_flow","score":score,"status":band(score),"weight":WEIGHTS["energy_port_flow"],"available":True,"retained":False,"coverage":len(rows),"method":"Energy-port calls: seven-day mean versus preceding 28 days, reliability-shrunk.","evidence":sorted(rows,key=lambda x:x["pressure"],reverse=True)}
def fred(series_id:str,label:str,lag:int,days:int)->dict[str,Any]:
 lines=get(FRED,params={"id":series_id,"cosd":(date.today()-timedelta(days=days)).isoformat()}).text.splitlines();pts=[]
 for line in lines[1:]:
  a=line.split(',');d=pdate(a[0] if a else None)
  if d and len(a)>1 and a[1] not in ("","."):pts.append((d,num(a[1])))
 returns=[(pts[i][1]/pts[i-lag][1]-1)*100 for i in range(lag,len(pts)) if pts[i-lag][1]>0];z=rz(returns[-1],returns[-51:-1]);score=clamp((abs(z)-.5)/2.5*100)
 return {"id":series_id,"label":label,"latest_date":pts[-1][0].isoformat(),"latest_value":round(pts[-1][1],2),"change_pct":round(returns[-1],2),"robust_z":round(z,2),"score":round(score,1),"url":f"https://fred.stlouisfed.org/series/{series_id}"}
def collect_markets(now:datetime)->dict[str,Any]:
 with ThreadPoolExecutor(max_workers=2) as pool:a=pool.submit(fred,"DCOILBRENTEU","Brent crude",5,300);b=pool.submit(fred,"PNGASEUUSDM","European natural gas",1,1500);rows=[a.result(),b.result()]
 score=round(.7*rows[0]["score"]+.3*rows[1]["score"],1);return {"key":"energy_markets","score":score,"status":band(score),"weight":WEIGHTS["energy_markets"],"available":True,"retained":False,"coverage":2,"method":"Absolute robust-z of five-observation Brent and one-month gas returns.","evidence":rows}
def fetch_ofac(page:int)->list[dict[str,Any]]:
 soup=BeautifulSoup(get(OFAC,params={"page":page}).text,"html.parser");out=[]
 for tr in soup.select("table tr"):
  cells=tr.select("td");links=tr.select("a[href]")
  if len(cells)>=3:out.append({"title":" ".join(cells[0].get_text(" ",strip=True).split()),"action":" ".join(cells[1].get_text(" ",strip=True).split()),"date":cells[2].get_text(" ",strip=True)[:10],"url":links[0].get("href") if links else OFAC})
 return out
def collect_ofac(now:datetime)->dict[str,Any]:
 rows=[]
 with ThreadPoolExecutor(max_workers=6) as pool:
  for f in as_completed([pool.submit(fetch_ofac,p) for p in range(10)]):rows.extend(f.result())
 weeks=[0.0]*14;evidence=[]
 for r in {f"{x['date']}|{x['title']}":x for x in rows}.values():
  d=pdate(r["date"]);age=(now.date()-d).days if d else 999;text=f"{r['title']} {r['action']}".lower();regions=[x for x in REGION if x in text]
  if not 0<=age<98 or not regions:continue
  terms=[x for x in ENERGY if x in text];actions=[x for x in ACTION if x in text];weight=1+sum(ACTION[x] for x in actions)+(1 if terms else 0);weeks[age//7]+=weight;evidence.append({**r,"region_terms":regions,"energy_terms":terms,"action_terms":actions,"weight":round(weight,2),"age":age})
 cur=avg(weeks[:2]);z=rz(cur,weeks[2:]);score=round(.55*clamp(cur*6,hi=55)+.45*clamp(max(0,z)*18,hi=45),1);return {"key":"regional_sanctions","score":score,"status":band(score),"weight":WEIGHTS["regional_sanctions"],"available":True,"retained":False,"coverage":len(evidence),"current_14d_weekly_equivalent":round(cur,2),"anomaly_z":round(z,2),"method":"MENA-linked OFAC action velocity over 14 days versus 12 prior weeks.","evidence":sorted([x for x in evidence if x["age"]<35],key=lambda x:x["date"],reverse=True)[:12]}
def weather_one(p:dict[str,Any],now:datetime)->dict[str,Any]:
 data=get(MET,params={"lat":p["lat"],"lon":p["lon"]}).json();winds=[];rain={};valid=None
 for x in (data.get("properties") or {}).get("timeseries",[]):
  t=pdt(x.get("time"))
  if not t or t<now-timedelta(hours=3) or t>now+timedelta(days=7):continue
  detail=(((x.get("data") or {}).get("instant") or {}).get("details") or {});winds.append(num(detail.get("wind_speed")));period=(x.get("data") or {}).get("next_1_hours") or (x.get("data") or {}).get("next_6_hours") or {};rain[t.date().isoformat()]=rain.get(t.date().isoformat(),0)+num((period.get("details") or {}).get("precipitation_amount"));valid=x.get("time")
 wind=max(winds);precip=max(rain.values(),default=0);score=max(clamp((wind-12)/14*100),clamp((precip-30)/90*100));return {**p,"max_wind_ms":round(wind,1),"max_precip_24h_mm":round(precip,1),"pressure":round(score,1),"valid_to":valid}
def collect_weather(ports:list[dict[str,Any]],now:datetime)->dict[str,Any]:
 with ThreadPoolExecutor(max_workers=6) as pool:rows=list(pool.map(lambda p:weather_one(p,now),ports[:8]));rows.sort(key=lambda x:x["pressure"],reverse=True);score=round(.65*avg([x["pressure"] for x in rows[:3]])+.35*avg([x["pressure"] for x in rows]),1);return {"key":"port_weather","score":score,"status":band(score),"weight":WEIGHTS["port_weather"],"available":True,"retained":False,"coverage":len(rows),"method":"Seven-day maximum wind and daily precipitation at eight energy ports.","evidence":rows}
def previous()->dict[str,Any]:
 try:return json.loads(OUTPUT.read_text(encoding="utf-8"))
 except (OSError,json.JSONDecodeError):return {}
def fallback(old:dict[str,Any],key:str,now:datetime,e:Exception)->dict[str,Any]:
 gen=pdt((old.get("meta") or {}).get("generated"));row=(old.get("components") or {}).get(key)
 if gen and timedelta(0)<=now-gen<=timedelta(hours=FALLBACK_HOURS) and isinstance(row,dict) and row.get("available"):
  x=json.loads(json.dumps(row));x["retained"]=True;x["retained_reason"]=type(e).__name__;return x
 return {"key":key,"score":None,"status":"UNAVAILABLE","weight":WEIGHTS[key],"available":False,"retained":False,"coverage":0,"evidence":[],"error":type(e).__name__}
def main()->None:
 now=datetime.now(timezone.utc);old=previous();ports=port_directory();collectors={"chokepoint_flow":lambda:collect_chokes(now),"energy_port_flow":lambda:collect_ports(ports,now),"energy_markets":lambda:collect_markets(now),"regional_sanctions":lambda:collect_ofac(now),"port_weather":lambda:collect_weather(ports,now)};components={};notes=[]
 for k,fn in collectors.items():
  try:components[k]=fn();print(f"[live] {k}: {components[k]['score']}")
  except Exception as e:components[k]=fallback(old,k,now,e);notes.append(f"{k}: {'retained' if components[k].get('retained') else 'unavailable'} ({type(e).__name__})");print(f"[fallback] {k}: {e}")
 avail=[x for x in components.values() if x.get("available") and x.get("score") is not None];den=sum(x["weight"] for x in avail);raw=sum(x["score"]*x["weight"] for x in avail)/den if den else 0;physical=any(num(components.get(k,{}).get("score"))>=40 for k in ("chokepoint_flow","energy_port_flow","port_weather"));independent=any(num(components.get(k,{}).get("score"))>=45 for k in ("energy_markets","regional_sanctions"));bonus=5.0 if physical and independent else 0;score=round(clamp(raw+bonus),1);status=band(score);coverage=len(avail);retained=sum(1 for x in avail if x.get("retained"));confidence="HIGH" if coverage==5 and not retained else "MEDIUM" if coverage>=4 else "LOW";generated=now.isoformat();hist=[x for x in old.get("history",[]) if isinstance(x,dict) and x.get("generated")];hist.append({"generated":generated,"score":score,"status":status})
 out={"meta":{"project":"mena-energy-flow","generated":generated,"mode":"live" if coverage==5 and not retained else "partial","version":"2.0.0","horizon":"0–14 days","classification":"energy-flow-disruption-pressure-not-price-or-conflict-probability","coverage":f"{coverage}/5","confidence":confidence,"source_notes":notes},"warning":{"score":score,"raw_score":round(raw,1),"concurrence_bonus":bonus,"status":status,"headline":f"MENA energy-flow disruption pressure is {status.lower()} at {score:.1f}/100.","interpretation":"The index combines physical chokepoint and port-flow anomalies with independent market, sanctions and forward-weather evidence. It is a screening warning, not a price or conflict probability."},"components":components,"history":hist[-60:],"sources":SOURCES,"methodology":{"weights":WEIGHTS,"fallback_hours":FALLBACK_HOURS,"concurrence_rule":"+5 when physical flow/weather ≥40 and market/sanctions ≥45"}}
 OUTPUT.write_text(json.dumps(out,indent=2,ensure_ascii=False),encoding="utf-8");print(f"score={score} status={status} coverage={coverage}/5 confidence={confidence}")
if __name__=="__main__":main()
