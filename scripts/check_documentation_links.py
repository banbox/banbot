from pathlib import Path
import re,subprocess,sys
from urllib.parse import unquote
ROOT=Path(__file__).resolve().parents[1]
def candidates(source,target):
    if target.startswith("/"):
        base=ROOT/"bandoc"
        p=base/target.lstrip("/")
        yield p
        yield base/"public"/target.lstrip("/")
    else:
        p=source.parent/target
        yield p
    if not p.suffix:
        yield p.with_suffix(".md")
        yield p/"index.md"
    elif p.suffix==".html":
        yield p.with_suffix(".md")
def main():
    files=subprocess.check_output(["git","ls-files","--cached","--others","--exclude-standard","--","*.md"],cwd=ROOT,text=True,encoding="utf-8").splitlines()
    sources=sorted(set(files))
    errors=[]; checked=0
    for name in sources:
        source=ROOT/name
        if not source.exists():continue
        text=source.read_text(encoding="utf-8-sig")
        text=re.sub(r"(?ms)^\s*\x60{3}.*?^\s*\x60{3}\s*$","",text)
        links=re.findall(r"!?(?:\[[^\]]*\])\(([^\n]*?)\)",text)
        links+=re.findall(r"(?m)^\s*\[[^\]]+\]:\s*(\S+)",text)
        for raw in links:
            target=raw.strip()
            if target.startswith("<"):target=target.split(">")[0][1:]
            else:target=target.split()[0] if target else ""
            target=unquote(target.split("#")[0].split("?")[0])
            if not target or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:",target) or "$" in target or "{" in target:continue
            # Onsite routes are resolved against VitePress src/public, not the repository root.
            checked+=1
            if not any(p.exists() for p in candidates(source,target)):errors.append(f"{name}: {target}")
    # Resolve static routes and the shared menu's ${prefix}/... API template.
    for cfg in (ROOT/"bandoc/.vitepress/config").glob("*"):
        if cfg.suffix not in (".js",".mts"):continue
        config_text=cfg.read_text(encoding="utf-8")
        targets=re.findall(r"link:\s*['\"](/[^'\"]+)['\"]",config_text)
        for suffix in re.findall(r"link:\s*\x60\$\{prefix\}(/[^\x60]+)\x60",config_text):
            targets.extend(f"/{language}/api{suffix}" for language in ("en-US","zh-CN"))
        for target in targets:
            checked+=1
            if not any(p.exists() for p in candidates(cfg,target)):errors.append(f"{cfg.relative_to(ROOT)}: {target}")
    print(f"Checked {len(sources)} Markdown paths and {checked} local file/site references; fragment anchors and external URLs are outside this check.")
    if errors:
        print("\n".join(errors)); return 1
    print("PASS: local Markdown and VitePress routes/assets exist");return 0
if __name__=="__main__":sys.exit(main())
