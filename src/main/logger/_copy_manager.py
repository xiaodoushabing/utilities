"""
CopyManager - Handles file copying operations and thread management.

This module provides background file copying functionality with thread management,
retry logic, and signal handling.
"""

import os
import glob
import threading
import time
import signal
from typing import Optional, List, Set, Dict, Any
from pathlib import Path

from ..file_io import FileIOInterface


class CopyManager:
    """
    CopyManager handles background file copying operations.
    
    This class manages multiple concurrent copy operations, each running in its own
    thread with configurable retry logic and error handling.
    """
    
    def __init__(self, enabled: bool = True):
        """
        Initialize CopyManager.
        
        Args:
            enabled (bool): Whether copy functionality is enabled. Default is True.
        """
        self._enabled = enabled
        self._copy_threads: Dict[str, threading.Thread] = {}            # thread_name -> thread object
        self._stop_events: Dict[str, threading.Event] = {}              # thread_name -> threading.Event
        self._copy_operations_files: Dict[str, Set[str]] = {}           # copy_name -> set of files being copied
        self._copy_operations_params: Dict[str, Dict[str, Any]] = {}    # copy_name -> copy parameters dict
        self._shutdown_in_progress = False                              # flag to prevent new copy operations during shutdown

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            """Modify signal handler to perform cleanup."""
            print(f"\nReceived signal '{signum}'. Stopping all copy operations and cleaning up...")
            self._copy_operations(timeout=60.0)
            # restore default handler and re-raise signal
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
        
        # register modified handler for common termination signals
        if hasattr(signal, 'SIGTERM'):
            try:
                signal.signal(signal.SIGINT, signal_handler)
                signal.signal(signal.SIGTERM, signal_handler)
                print("Signal handlers registered")
            except Exception as e:
                print(
                    f"Could not register signal handlers: {e}.\n"
                    f"CopyManager will rely on external cleanup instead."
                )

    def is_enabled(self) -> bool:
        """Check if copy functionality is enabled."""
        return self._enabled
    
    def start_copy(
        self,
        copy_name: str,
        path_patterns: List[str],
        copy_destination: str,
        root_dir: Optional[str] = None,
        copy_interval: int = 60,
        create_dest_dirs: bool = True,
        preserve_structure: bool = False,
        max_retries: int = 3,
        retry_delay: int = 5
    ) -> None:
        """
        Start a background thread to periodically copy log files from local to destination.
        
        Args:
            copy_name (str): Unique name for this copy operation (used for thread identification).
            path_patterns (List[str]): Glob pattern(s) or file path(s) to match local files.
                Examples: 
                - ["/path/to/logs/*.log"]
                - ["/path/to/logs/*.log", "/path/to/logs/*.txt"]
                - ["/path/to/specific/file.log"]
            copy_destination (str): destination directory path.
                Example: "hdfs://namenode:port/path/to/hdfs/logs/"
            root_dir (Optional[str]): Root directory for the local files.
            copy_interval (int): Interval in seconds between copy operations. Default is 60 seconds.
            create_dest_dirs (bool): Whether to create destination directories if they don't exist.
            preserve_structure (bool): Whether to preserve local directory structure in destination.
                If True, root_dir must be specified.
                If True: "/local/logs/app/file.log" -> "hdfs://dest/app/file.log"
                If False: "/local/logs/app/file.log" -> "hdfs://dest/file.log"
            max_retries (int): Maximum number of retry attempts for failed copies. Default is 3.
            retry_delay (int): Delay in seconds between retry attempts. Default is 5.
            
        Raises:
            ValueError: If copy_name already exists or parameters are invalid.
            
        Example:
            # Copy all .log files every 2 minutes
            copy_manager.start_copy(
                copy_name="app_logs",
                path_patterns="/tmp/logs/*.log",
                copy_destination="hdfs://namenode:9000/logs/app/",
                copy_interval=120
            )
            
            # Manually trigger additional copy if needed
            copy_manager.trigger_copy_now("app_logs")

        Copy Behavior:
            - Periodic copy: Files are copied every copy_interval seconds  
            - Final copy: Files are copied one last time during cleanup/shutdown
            - Manual copy: Files can be copied on-demand using trigger_copy_now()
        """
        # Validate parameters
        if not copy_name:
            raise ValueError("copy_name cannot be empty")
        if self._shutdown_in_progress:
            raise ValueError("Cannot start new copy operations: CopyManager is shutting down")

        # Check copy enablement
        if not self._enabled:
            print(f"Copy operation '{copy_name}' skipped: disabled in system environment")
            return
        
        if copy_name in self._copy_threads:
            raise ValueError(f"Copy operation '{copy_name}' already exists. Use stop_copy() first.")
        if not path_patterns:
            raise ValueError("path_patterns cannot be empty")
        if not copy_destination:
            raise ValueError("copy_destination cannot be empty")
        if copy_interval <= 0:
            raise ValueError("copy_interval must be positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if retry_delay < 0:
            raise ValueError("retry_delay cannot be negative")
        if preserve_structure and not root_dir:
            raise ValueError(
                "'root_dir' must be specified when 'preserve_structure' is True. "
                "This is required to maintain the directory structure."
            )
        
        # Create stop event for this copy operation
        stop_event = threading.Event()
        self._stop_events[copy_name] = stop_event
        
        # Create and start the copy thread
        copy_thread = threading.Thread(
            target=self._copy_worker,
            args=(
                copy_name,
                path_patterns,
                copy_destination,
                copy_interval,
                create_dest_dirs,
                preserve_structure,
                root_dir,
                max_retries,
                retry_delay,
                stop_event
            ),
            daemon=False,
            name=f"Copy-{copy_name}"
        )
        
        self._copy_threads[copy_name] = copy_thread
        self._copy_operations_files[copy_name] = set()  # Initialize empty file set
        
        # Store copy parameters for later use (e.g., manual triggers, final copy)
        self._copy_operations_params[copy_name] = {
            'path_patterns': path_patterns,
            'copy_destination': copy_destination,
            'create_dest_dirs': create_dest_dirs,
            'preserve_structure': preserve_structure,
            'root_dir': root_dir,
            'max_retries': max_retries,
            'retry_delay': retry_delay
        }
        
        copy_thread.start()
        
        print(f"Started copy operation '{copy_name}' with {copy_interval}s interval.\n")

    def stop_copy(self, copy_name: str, timeout: float = 60.0) -> bool:
        """
        Stop a running copy operation.
        
        Args:
            copy_name (str): Name of the copy operation to stop.
            timeout (float): Maximum time to wait for thread to stop. Default is 10 seconds.
            
        Returns:
            bool: True if successfully stopped, False if timeout occurred.
            
        Raises:
            ValueError: If copy_name doesn't exist.
        """
        if copy_name not in self._copy_threads:
            raise ValueError(f"Copy operation '{copy_name}' does not exist")
        
        # Signal the thread to stop
        self._stop_events[copy_name].set()
        
        # Wait for thread to finish
        self._copy_threads[copy_name].join(timeout=timeout)
        
        # Check if thread actually stopped
        if self._copy_threads[copy_name].is_alive():
            print(
                f"Warning: Copy thread '{copy_name}' did not stop within {timeout}s.\n"
                f"Consider increasing the timeout or checking the thread manually."
            )
            return False
        
        del self._copy_threads[copy_name]
        del self._stop_events[copy_name]
        del self._copy_operations_files[copy_name]  # Clean up file tracking
        del self._copy_operations_params[copy_name] # Clean up parameter storage
        
        print(f"Stopped copy operation '{copy_name}'")
        return True

    def stop_all_copy_operations(self, timeout: float = 60.0, verbose: bool = False) -> List[str]:
        """
        Stop all running copy operations.
        
        Args:
            timeout (float): Maximum time to wait for each thread to stop. Defaults to 30 seconds.
            verbose (bool): Whether to provide detailed feedback during the operation. Defaults to False.
            
        Returns:
            List[str]: Names of copy operations that failed to stop within timeout.
        """
        if not self._copy_threads:
            return []
            
        if verbose:
            print(f"Stopping {len(self._copy_threads)} copy operation(s)...")
            
        failed_to_stop = []
        copy_names = list(self._copy_threads.keys())
        
        for copy_name in copy_names:
            try:
                if not self.stop_copy(copy_name, timeout):
                    failed_to_stop.append(copy_name)
            except ValueError as e:
                print(f"Error stopping copy operation '{copy_name}': {e}")
                failed_to_stop.append(copy_name)
        
        if verbose:
            if failed_to_stop:
                print(
                    f"WARNING: Some copy operation(s) did not stop cleanly: {failed_to_stop}\n"
                    f"Please check the threads manually."
                )
            else:
                print("All copy operation(s) stopped successfully.")
                
        return failed_to_stop

    def list_copy_operations(self) -> List[dict]:
        """
        List all active copy operation(s).
        
        Returns:
            List[dict]: Information about active copy operations.
        """
        operations = []
        for copy_name, thread in self._copy_threads.items():
            operations.append({
                "name": copy_name,
                "thread_name": thread.name,
                "is_alive": thread.is_alive(),
                "daemon": thread.daemon
            })
        return operations
    
    def trigger_copy_now(self, copy_name: Optional[str] = None) -> None:
        """
        Manually trigger an immediate copy operation for specific or all operations.
        
        This method allows you to force a copy operation outside of the normal
        interval schedule. Useful for ensuring log files are copied before
        critical operations or during testing.
        
        Args:
            copy_name (Optional[str]): Name of specific copy operation to trigger.
                                     If None, triggers all active operations.
                                     
        Raises:
            ValueError: If copy_name is specified but doesn't exist.
            
        Example:
            # Trigger specific operation
            copy_manager.trigger_copy_now("my_copy_operation")
            
            # Trigger all operations  
            copy_manager.trigger_copy_now()
        """
        if not self._copy_threads:
            print("No active copy operations to trigger.")
            return
            
        if copy_name is not None:
            for name in copy_name:
                if name not in self._copy_threads:
                    raise ValueError(f"Copy operation '{name}' does not exist")
            copy_names = copy_name
        else:
            copy_names = list(self._copy_threads.keys())
            
        print(f"Manually triggering {len(copy_names)} copy operation(s)...")
        
        for name in copy_names:
            try:
                # Use stored parameters instead of extracting from thread args
                params = self._copy_operations_params[name]
                self._perform_copy_operation(
                    name,
                    params['path_patterns'],
                    params['copy_destination'],
                    params['create_dest_dirs'],
                    params['preserve_structure'],
                    params['root_dir'],
                    params['max_retries'],
                    params['retry_delay']
                )
                    
            except Exception as e:
                print(f"Exception occured during manually-triggered copy operation for '{name}': {e}")

    def _copy_worker(
        self,
        copy_name: str,
        path_patterns: List[str],
        copy_destination: str,
        copy_interval: int,
        create_dest_dirs: bool,
        preserve_structure: bool,
        root_dir: Optional[str],
        max_retries: int,
        retry_delay: int,
        stop_event: threading.Event
    ) -> None:
        """
        Worker function that runs in a separate thread to perform periodic copying.
        """
        print(f"Copy worker '{copy_name}' started.")

        while not stop_event.is_set():
            # Perform periodic copy
            self._perform_copy_operation(
                copy_name, 
                path_patterns,
                copy_destination,
                create_dest_dirs,
                preserve_structure,
                root_dir,
                max_retries,
                retry_delay
            )

            # Wait for the interval
            if stop_event.wait(timeout=copy_interval):
                break

        print(f"Copy worker '{copy_name}' stopped")
        
    def _perform_copy_operation(
        self,
        copy_name: str,
        path_patterns: List[str],
        copy_destination: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        root_dir: Optional[str],
        max_retries: int,
        retry_delay: int
    ) -> None:
        """
        Perform a single copy operation.
        
        Args:
            copy_name (str): Name of the copy operation for logging.
            path_patterns (List[str]): File patterns to search for.
            copy_destination (str): Copy destination path.
            create_dest_dirs (bool): Whether to create destination directories.
            preserve_structure (bool): Whether to preserve directory structure.
            root_dir (Optional[str]): Root directory for relative paths.
            max_retries (int): Maximum number of retry attempts.
            retry_delay (int): Delay between retries in seconds.
        """
        try:
            # Find files matching the provided patterns
            files_to_copy = self._discover_files_to_copy(path_patterns)
            
            if files_to_copy:
                print(f"Copy '{copy_name}' found {len(files_to_copy)} files to copy.")
                
                # Check for duplicate files across operations and issue warnings
                self._check_for_duplicate_files(copy_name, files_to_copy)
                
                self._copy_files_to_dest(
                    files_to_copy,
                    copy_destination,
                    create_dest_dirs,
                    preserve_structure,
                    root_dir,
                    max_retries,
                    retry_delay
                )
            else:
                print(
                    f"Copy '{copy_name}': No files found matching patterns {path_patterns}. "
                    f"No files copied in this cycle."
                )
                
        except Exception as e:
            print(f"Error in copy operation '{copy_name}': {e}")

    def _discover_files_to_copy(self, path_patterns: List[str]) -> List[str]:
        """
        Discover files matching the provided patterns.
        
        Args:
            path_patterns (List[str]): List of file paths or glob patterns.
            
        Returns:
            List[str]: List of unique file paths that exist.
        """
        files_to_copy = []
        
        for pattern in path_patterns:
            if os.path.isfile(pattern):
                files_to_copy.append(pattern)
            else:
                try:
                    matched_files = glob.glob(pattern, recursive=True)
                    files_to_copy.extend([f for f in matched_files if os.path.isfile(f)])
                except (OSError, ValueError) as e:
                    print(f"Warning: Invalid pattern '{pattern}': {e}")
        
        return list(set(files_to_copy))

    def _check_for_duplicate_files(self, copy_name: str, files_to_copy: List[str]) -> None:
        """
        Check if any files in files_to_copy are already being copied by other operations.
        Issues warnings for duplicate files but doesn't prevent copying.
        
        Args:
            copy_name (str): Name of the current copy operation.
            files_to_copy (List[str]): List of files that this operation wants to copy.
        """
        if not files_to_copy:
            return
            
        current_files = set(files_to_copy)
        
        # Check against all other active operations
        for other_copy_name, other_files in self._copy_operations_files.items():
            if other_copy_name == copy_name:
                continue
                
            overlapping_files = current_files.intersection(other_files)
            if overlapping_files:
                print(
                    f"WARNING: copy operation '{copy_name}' and '{other_copy_name}' "
                    f"are both copying {len(overlapping_files)} file(s):"
                )
                for file_path in sorted(overlapping_files):
                    print(f"  - {file_path}")
                print(
                    f"This may cause race conditions or unnecessary resource usage. "
                    f"Consider adjusting your copy operation patterns to avoid overlaps.\n"
                )

        # Update the file set for this operation
        self._copy_operations_files[copy_name] = current_files

    def _copy_files_to_dest(
        self,
        local_files: List[str],
        copy_destination: str,
        create_dest_dirs: bool,
        preserve_structure: bool,
        root_dir: Optional[str],
        max_retries: int,
        retry_delay: int
    ) -> None:
        """
        Copy a list of local files to target destination.
        """
        success_count = 0
        error_count = 0
        
        for local_file in local_files:
            if preserve_structure:
                rel_path = os.path.relpath(local_file, root_dir)
                dest_path = os.path.join(copy_destination, rel_path).replace("\\", "/")
            else:
                filename = os.path.basename(local_file)
                dest_path = os.path.join(copy_destination, filename).replace("\\", "/")
            
            if create_dest_dirs:
                dest_dir = os.path.dirname(dest_path)
                try:
                    FileIOInterface.fmakedirs(dest_dir, exist_ok=True)
                except Exception as e:
                    print(f"Warning: Could not create directory {dest_dir}: {e}")

            for attempt in range(max_retries + 1):
                try:
                    # Copy file using FileIO interface
                    FileIOInterface.fcopy(
                        read_path=local_file,
                        dest_path=dest_path,
                    )
                    
                    success_count += 1
                    print(f"Successfully copied {local_file} -> {dest_path}")
                    break
                    
                except Exception as e:
                    if attempt < max_retries:
                        print(f"Attempt {attempt + 1} failed for {local_file}: {e}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"Failed to copy {local_file} after {max_retries + 1} attempts: {e}")
                        error_count += 1
        
        if success_count > 0 or error_count > 0:
            print(f"Copy completed: {success_count} successful, {error_count} failed")

    ## ------------------------------ CLEANUP ------------------------------ ##
    def cleanup(self, timeout: float = 60.0):
        """
        Cleanup function to stop all copy operations.
        
        This method performs a final copy operation for all active copy operations 
        before stopping them.
        
        Args:
            timeout (float): Timeout in seconds for stopping copy operations.
                                    Defaults to 60.0.
        
        Note: This method can be called multiple times safely.
        """
        if self._shutdown_in_progress:
            return
        
        self._shutdown_in_progress = True
        
        print("CopyManager cleanup initiated...")
        
        # Perform final copy operation before stopping threads
        if self._copy_threads:
            print("Performing final copy before shutdown...")
            try:
                self.trigger_copy_now()
            except Exception as e:
                print(f"Warning: Final copy failed during cleanup: {e}")
        
        self.stop_all_copy_operations(timeout=timeout, verbose=True)
        self._copy_operations_files.clear()
        self._copy_operations_params.clear()
        print("CopyManager cleanup completed.")
