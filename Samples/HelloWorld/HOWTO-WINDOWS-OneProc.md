# How to run the HelloWorld Sample (Integrated IC on Windows with .NET Core)

This sample shows two immortals communicating, a client and a server. You can build and run it locally to get a quick idea of how Ambrosia operates. The solution contains three alternate versions of the client (Client1, Client2 and Client3), only one of which is used at a time.  Client1 demonstrates basic communication, Client2 demonstrates nondeterministic input using an impulse handler and Client3 demonstrates how 2 way commmunication is done.

## Quick Build

The sample uses the AmbrosiaLibCS NuGet package to import the necessary Ambrosia binaries. These packages are picked up automatically from the public NuGet feed, so there are no extra steps required to import them.

- Build `HelloWorld.sln` in Visual Studio, using configuration `Debug` and platform `Any CPU`.

As part of a full build, Ambrosia generates code for the interface proxies. To make this sample easy to run, we have already included the generated code, so you don't have to run code generation just to build and run the HelloWorld sample. However, if you want to experiment with it or make any changes to the interfaces, you can run code generation yourself as described in the section "Full Build w/ Code Generation" below.

### Getting the Ambrosia tool binaries

To run HelloWorld you'll also need the Ambrosia tools that are distributed in a compressed folder. To get these,

- Download the compressed folder `Ambrosia-win-x64.zip` from [Releases](https://github.com/microsoft/AMBROSIA/releases)
- Unpack it somewhere on your disk; for example, `C:\Ambrosia-win-x64\`
- Set the `%AMBROSIATOOLS%` environment variable to point to that directory

## Running HelloWorld 

For the purpose of this tutorial, we'll assume the following parameters:

- Log directory: `C:\logs\`
- Client instance name: `client`
- Client ImmortalCoordinator CRA port: `1500`
- Server instance name: `server`
- Server ImmortalCoordinator CRA port: `2500`

### Storage Connection String

Ambrosia uses an Azure table to maintain and discover information about your application's immortals and their status. To access this information, all Ambrosia tools and libraries need a connection string to an Azure storage account stored in the environment variable `%AZURE_STORAGE_CONN_STRING%`. To run this sample, you must therefore create an Azure Storage account if you don't already have one, and set the environment variable to contain its connection string (Please refer to following [link](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) for more details).

### Registering the Immortal instances

Before running the application, you need to register each Immortal instance
so that other Immortal instances can find them. Open a command prompt and enter the following commands:

```bat
cd %AMBROSIATOOLS%\x64\Release\netcoreapp3.1\
dotnet Ambrosia.dll RegisterInstance -i=client -rp=1000 -sp=1001 -l=C:\logs\
dotnet Ambrosia.dll RegisterInstance -i=server -rp=2000 -sp=2001 -l=C:\logs\
```

When each of these completes successfully, you will see the following message, one for each ```RegisterInstance```.

```
The CRA instance appears to be down. Restart it and this vertex will be instantiated automatically.
```

This message indicates that other instances can connect to them and even perform method calls reliably, although the methods won't actually execute until the instances are started. 

Note that the values specified in the rp and sp flags are not used in integrated IC mode, but are still needed for separate IC mode. We are considering eliminating the requirement for these parameter values in a future release. 

### Running the application

To run the HelloWorld application, you will need to run two command-line
processes, each in a separate console window: the HelloWorld client Immortal, and the
HelloWorld server Immortal.

To run the HelloWorld server, in the first console window:

```bat
cd Server\bin\x64\Debug\netcoreapp3.1
dotnet Server.dll
```

To run the HelloWorld client, in the second console window:

```bat
cd Client1\bin\x64\Debug\netcoreapp3.1
dotnet Client1.dll
```

After starting both processes, you should see your client and server
communicate with each other! Specifically:

- The console of the server process prints `SERVER Received message from a client: Hello World 1!`
- The console of the client process prints `Client: Sent message 1` and `Press enter to continue`
- (Now press enter in the client process console window)
- The console of the server process prints `SERVER Received message from a client: Client: Hello World 2!`
- The console of the server process prints `SERVER Received message from a client: Client: Hello World 3!`

### Clearing state and re-running

If you want to run Hello World a second time, it is not enough to just restart the immortals! They will just resume running where they left off (at the end of Hello World, or wherever else they were). So, to start over, you have to clear the state. It's simple:

- Delete the contents of the `C:\logs` directory.

Optionally, you can also delete the registrations in the Azure table:

```bat
cd %AMBROSIATOOLS%\x64\Release\netcoreapp3.1\
dotnet UnsafeDeregisterInstance.dll server
dotnet UnsafeDeregisterInstance.dll client
```

Of course, if you delete those, you have to re-register them in order to run HelloWorld again.

## Full Build w/ Code Generation *

Ambrosia generates code for the interface proxies. To make this sample easy to run, we have already included the generated code, so you don't have to run code generation just to build and run the HelloWorld sample. However, if you want to experiment with it or make any changes to the interfaces, here is how you can run the code generation step.

1. Build the projects `IClient1`, `IClient2`, `IClient3` and `IServer`. This generates the binaries used by the code generation step.

2. Run code generation by executing `Generate-Assemblies-NetCore.ps1`. This overwrites the content of the projects `Client1Interfaces`, `Client2Interfaces`. `Client3Interfaces` and `ServerInterfaces` with generated code.

3. Build HelloWorld.sln. This now picks up the freshly generated source files.

* Important note: If you are running Client3, build stages 1, 3 in `Debug` Configuration. At this time we require any Immortal making Async calls (in contrast to Fork calls), and the associated generated project, to be compiled in `Debug` configuration mode. 
This is due to a current limitation in our ability to serialize .NET tasks which will hopefully be overcome in the future.
