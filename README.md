# supercaster
Transfer UDP multicast packets to gRPC stream and vice-versa.

Public cloud providers (such as AWS or Google Cloud Platform) do not support multicast routing.
So, if an application A on a VM 1 sends UDP multicast packets, another application B running on VM 2 will not be able to receive them.

A solution may be [Weave Net](https://www.weave.works/oss/net/), but it is not available for Windows.

So, the solution is to use supercaster on both VMs:
- launch supercaster on VM 1 in mode multicast-to-gRPC, in order to forward received UDP multicast packets to a gRPC stream,
- launch supercaster on VM 2 in mode gRPC-to-multicast, in order to forward received gRPC messages to UDP multicast destination.

Application A on VM1 ->*multicast*-> supercaster on VM1 ->*grpc*-> supercaster on VM2 ->*multicast*-> application B on VM2

