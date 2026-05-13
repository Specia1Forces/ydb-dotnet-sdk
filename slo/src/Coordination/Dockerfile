FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["slo/src/Coordination/Coordination.Slo.csproj", "slo/src/Coordination/"]
COPY ["slo/src/Internal/Internal.csproj", "slo/src/Internal/"]
COPY ["src/Ydb.Sdk/src/Ydb.Sdk.csproj", "src/Ydb.Sdk/src/"]

RUN dotnet restore "slo/src/Coordination/Coordination.Slo.csproj"

COPY . .

WORKDIR "/src/slo/src/Coordination"
RUN dotnet build "Coordination.Slo.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Coordination.Slo.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Coordination.Slo.dll"]
