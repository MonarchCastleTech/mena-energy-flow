from datetime import datetime,timedelta,timezone
from pipeline.mena_warning_model import band,clamp,fallback,pressure,rz
def test_bounds():assert clamp(-1)==0 and clamp(101)==100
def test_bands():assert [band(x) for x in (0,25,45,65,80)]==['BASELINE','WATCH','ELEVATED','HIGH','SEVERE']
def test_pressure_flat():
 p,c,d,r=pressure([10]*35);assert p==0 and c==0 and d=='surge' and r==1
def test_pressure_anomaly():
 p,c,d,r=pressure([20]*7+[10]*28);assert p>90 and c==100 and d=='surge'
def test_low_volume_shrink():
 p,_,_,r=pressure([1]*7+[.1]*28);assert r<.3 and p<30
def test_robust_z():assert rz(10,[1,1,2,2,3,3])>3
def test_recent_component_fallback_is_keyless_and_expires():
 now=datetime.now(timezone.utc);component={"key":"energy_markets","score":48,"available":True,"weight":.2,"evidence":[]};recent={"meta":{"generated":(now-timedelta(hours=2)).isoformat()},"components":{"energy_markets":component}}
 kept=fallback(recent,"energy_markets",now,RuntimeError("offline"));assert kept["score"]==48 and kept["retained"] is True and component.get("retained") is None
 stale={"meta":{"generated":(now-timedelta(hours=73)).isoformat()},"components":{"energy_markets":component}};assert fallback(stale,"energy_markets",now,RuntimeError("offline"))["available"] is False

def test_retained_source_does_not_get_a_new_freshness_window():
 now=datetime.now(timezone.utc); first=now-timedelta(hours=71)
 previous={"meta":{"generated":first.isoformat()},"components":{"energy_markets":{"score":48,"available":True,"weight":.2}}}
 retained=fallback(previous,"energy_markets",now,RuntimeError("offline"))
 refreshed={"meta":{"generated":now.isoformat()},"components":{"energy_markets":retained}}
 assert fallback(refreshed,"energy_markets",now+timedelta(hours=2),RuntimeError("offline"))["available"] is False

def test_no_chokepoint_series_is_unavailable_not_zero(monkeypatch):
 import pytest
 from pipeline import mena_warning_model as model
 monkeypatch.setattr(model,"series",lambda *args:None)
 with pytest.raises(RuntimeError,match="No usable chokepoint series"):
  model.collect_chokes(datetime.now(timezone.utc))

def test_port_directory_failure_keeps_independent_sources(monkeypatch,tmp_path):
 import json
 from pipeline import mena_warning_model as model
 monkeypatch.setattr(model,"OUTPUT",tmp_path/"output.json")
 def fail():raise RuntimeError("port service unavailable")
 monkeypatch.setattr(model,"port_directory",fail)
 for key,name in [("chokepoint_flow","collect_chokes"),("energy_markets","collect_markets"),("regional_sanctions","collect_ofac")]:
  monkeypatch.setattr(model,name,lambda now,key=key:{"key":key,"score":20,"weight":model.WEIGHTS[key],"available":True,"retained":False})
 model.main();out=json.loads(model.OUTPUT.read_text(encoding="utf-8"))
 assert out["meta"]["mode"]=="partial" and out["meta"]["coverage"]=="3/5"
 assert not out["components"]["energy_port_flow"]["available"]
 assert not out["components"]["port_weather"]["available"]
 assert out["components"]["energy_markets"]["source_collected_at"]
