# Application branding

- **`logo.png`** — Source artwork for the Windows app and installer (place your PNG here, square or near-square works best).
- **`app.ico`** — Generated automatically by `build_exe.py` from `logo.png` (requires **Pillow**: `pip install pillow`). Do not edit by hand; rebuild the exe to refresh.

If `app.ico` is missing, run:

```bash
pip install pillow
python build_exe.py
```

The NSIS installer (`installer/AzureMigrationTool.nsi`) uses `..\resources\app.ico` for the setup wizard; run `build_exe.py` at least once before `makensis` so the icon exists.
