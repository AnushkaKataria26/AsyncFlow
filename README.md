# asyncflow

A distributed job queue and background processing system.

## Queue Core Server
The `queue_core` directory contains a high-performance C++ TCP job queue server. Note that because it relies on POSIX sockets, it currently targets Linux and Mac. Windows users can run this service via WSL (Windows Subsystem for Linux).
