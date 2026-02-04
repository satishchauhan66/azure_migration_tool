"""Setup module for Azure Migration Tool."""
from .auto_setup import ensure_dependencies, DependencyChecker, show_setup_dialog

__all__ = ['ensure_dependencies', 'DependencyChecker', 'show_setup_dialog']
