from pipeline.mena_warning_model import band,clamp,pressure,rz
def test_bounds():assert clamp(-1)==0 and clamp(101)==100
def test_bands():assert [band(x) for x in (0,25,45,65,80)]==['BASELINE','WATCH','ELEVATED','HIGH','SEVERE']
def test_pressure_flat():
 p,c,d,r=pressure([10]*35);assert p==0 and c==0 and d=='surge' and r==1
def test_pressure_anomaly():
 p,c,d,r=pressure([20]*7+[10]*28);assert p>90 and c==100 and d=='surge'
def test_low_volume_shrink():
 p,_,_,r=pressure([1]*7+[.1]*28);assert r<.3 and p<30
def test_robust_z():assert rz(10,[1,1,2,2,3,3])>3
