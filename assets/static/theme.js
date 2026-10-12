/* Loaded synchronously in <head>: applies the saved theme before the stylesheet paints. Storage may be unavailable. */
(()=>{
  const root=document.documentElement,key='seeded-theme';
  const apply=value=>{if(value==='light'||value==='dark')root.dataset.theme=value;else delete root.dataset.theme;};
  try{apply(localStorage.getItem(key));}catch(_){}
  root.classList.add('js');
  const os=matchMedia('(prefers-color-scheme: dark)');
  const resolved=()=>root.dataset.theme||(os.matches?'dark':'light');
  const reflect=()=>{
    const dark=resolved()==='dark',label=dark?'Switch to light theme':'Switch to dark theme';
    document.querySelectorAll('.theme-toggle').forEach(button=>{button.setAttribute('aria-pressed',String(dark));button.setAttribute('aria-label',label);button.title=label;});
  };
  os.addEventListener('change',reflect);
  addEventListener('storage',event=>{if(event.key===key||event.key===null){apply(event.newValue);reflect();}});
  document.addEventListener('click',event=>{
    if(!event.target.closest?.('.theme-toggle'))return;
    const value=resolved()==='dark'?'light':'dark';
    root.dataset.theme=value;
    try{localStorage.setItem(key,value);}catch(_){}
    reflect();
  });
  document.addEventListener('DOMContentLoaded',reflect);
})();
