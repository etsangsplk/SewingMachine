using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Health;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using SewingMachine.Impl;

namespace SewingMachine
{
    /// <summary>
    /// A public implementation of <see cref="IStateProviderReplica"/> based on <see cref="KeyValueStoreReplica"/>.
    /// </summary>
    public class KeyValueStateProvider : IStateProviderReplica
    {
        readonly TimeSpan backupCallbackExpectedCancellationTimeSpan = TimeSpan.FromSeconds(5.0);

        /// <summary>
        ///     Used to synchronize between backup callback invocation and replica close/abort
        /// </summary>
        readonly SemaphoreSlim backupCallbackLock;

        readonly TimeSpan healthInformationTimeToLive = TimeSpan.FromMinutes(5.0);
        readonly RestoreSettings restoreSettings;
        readonly bool userDefinedEnableIncrementalBackup;
        readonly LocalStoreSettings userDefinedLocalStoreSettings;
        readonly int? userDefinedLogTruncationInterval;
        readonly ReplicatorSettings userDefinedReplicatorSettings;
        CancellationTokenSource backupCallbackCts;

        Task<bool> backupCallbackTask;
        StatefulServiceInitializationParameters initParams;

        /// <summary>
        ///     Ensures single backup in progress at ActorStateProvider level.
        ///     This enables cleaning up the backup directory before invoking into KeyValueStoreReplica's backup.
        /// </summary>
        int isBackupInProgress;

        bool isClosingOrAborting;
        Func<CancellationToken, Task<bool>> onDataLossAsyncFunction;
        IStatefulServicePartition partition;
        ReplicaRole replicaRole;
        KeyValueStoreWrapper storeReplica;
        string traceId;

        /// <summary>
        ///     Creates an instance of <see cref="T:Microsoft.ServiceFabric.Actors.Runtime.KvsActorStateProvider" /> with default
        ///     settings.
        /// </summary>
        public KeyValueStateProvider()
            : this(null, null, false, new int?())
        {
        }

        /// <summary>
        ///     Creates an instance of <see cref="T:Microsoft.ServiceFabric.Actors.Runtime.KvsActorStateProvider" /> with specified
        ///     replicator and local key-value store settings.
        /// </summary>
        /// <param name="replicatorSettings">
        ///     A <see cref="T:System.Fabric.ReplicatorSettings" /> that describes replicator settings.
        /// </param>
        /// <param name="localStoreSettings">
        ///     A <see cref="T:System.Fabric.LocalStoreSettings" /> that describes local key value store settings.
        /// </param>
        public KeyValueStateProvider(ReplicatorSettings replicatorSettings = null,
            LocalStoreSettings localStoreSettings = null)
            : this(replicatorSettings, localStoreSettings, false, new int?())
        {
        }

        /// <summary>
        ///     Creates an instance of <see cref="T:Microsoft.ServiceFabric.Actors.Runtime.KvsActorStateProvider" /> with specified
        ///     settings.
        /// </summary>
        /// <param name="enableIncrementalBackup">
        ///     Indicates whether to enable incremental backup feature.
        ///     This sets the <see cref="P:System.Fabric.LocalEseStoreSettings.EnableIncrementalBackup" /> setting.
        /// </param>
        public KeyValueStateProvider(bool enableIncrementalBackup)
            : this(null, null, enableIncrementalBackup, new int?())
        {
        }

        /// <summary>
        ///     Creates an instance of <see cref="T:Microsoft.ServiceFabric.Actors.Runtime.KvsActorStateProvider" /> with specified
        ///     settings.
        /// </summary>
        /// <param name="enableIncrementalBackup">
        ///     Indicates whether to enable incremental backup feature.
        ///     This sets the <see cref="P:System.Fabric.LocalEseStoreSettings.EnableIncrementalBackup" /> setting.
        /// </param>
        /// <param name="logTruncationIntervalInMinutes">
        ///     Indicates the interval after which <see cref="T:System.Fabric.KeyValueStoreReplica" /> tries to truncate local
        ///     store logs.
        /// </param>
        /// <remarks>
        ///     When incremental backup is enabled for <see cref="T:System.Fabric.KeyValueStoreReplica" />, it does not use
        ///     circular buffer
        ///     to manage its transaction logs and periodically truncates the logs both on primary and secondary replica(s).
        ///     The process of taking backup(s) automatically truncates logs. On the primary replica, if no user backup
        ///     is initiated for <paramref name="logTruncationIntervalInMinutes" />,
        ///     <see cref="T:System.Fabric.KeyValueStoreReplica" />
        ///     automatically truncates the logs.
        /// </remarks>
        public KeyValueStateProvider(bool enableIncrementalBackup, int logTruncationIntervalInMinutes)
            : this(null, null, enableIncrementalBackup, logTruncationIntervalInMinutes)
        {
        }

        internal KeyValueStateProvider(ReplicatorSettings replicatorSettings,
            LocalStoreSettings localStoreSettings,
            bool enableIncrementalBackup, int? logTruncationIntervalInMinutes)
        {
            userDefinedReplicatorSettings = replicatorSettings;
            userDefinedLocalStoreSettings = localStoreSettings;
            userDefinedEnableIncrementalBackup = enableIncrementalBackup;
            userDefinedLogTruncationInterval = logTruncationIntervalInMinutes;
            replicaRole = ReplicaRole.Unknown;
            restoreSettings = new RestoreSettings(true);
            isBackupInProgress = 0;
            backupCallbackLock = new SemaphoreSlim(1);
            backupCallbackCts = null;
            backupCallbackTask = null;
            isClosingOrAborting = false;
        }

        public KeyValueStoreReplica StoreReplica => storeReplica;

        /// <summary>Function called during suspected data-loss.</summary>
        /// <value>A function representing data-loss callback function.</value>
        public Func<CancellationToken, Task<bool>> OnDataLossAsync
        {
            private get { return onDataLossAsyncFunction; }
            set
            {
                if (onDataLossAsyncFunction != null)
                    throw new InvalidOperationException("OnDataLossAsync has been already set.");
                onDataLossAsyncFunction = value;
            }
        }

        void IStateProviderReplica.Initialize(StatefulServiceInitializationParameters initParams)
        {
            traceId = initParams.PartitionId.ToString("B") + ":" +
                      initParams.ReplicaId.ToString(CultureInfo.InvariantCulture);
            this.initParams = initParams;
            storeReplica = CreateStoreReplica();
            storeReplica.Initialize(this.initParams);
        }

        Task<IReplicator> IStateProviderReplica.OpenAsync(ReplicaOpenMode openMode, IStatefulServicePartition partition,
            CancellationToken cancellationToken)
        {
            this.partition = partition;
            isBackupInProgress = 0;
            backupCallbackCts = null;
            backupCallbackTask = null;
            isClosingOrAborting = false;
            return storeReplica.OpenAsync(openMode, partition, cancellationToken);
        }

        async Task IStateProviderReplica.ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            await storeReplica.ChangeRoleAsync(newRole, cancellationToken);
            //if (newRole == ReplicaRole.Primary)
            //    logicalTimeManager.Start();
            //else
            //    logicalTimeManager.Stop();
            replicaRole = newRole;
        }

        async Task IStateProviderReplica.CloseAsync(CancellationToken cancellationToken)
        {
            await storeReplica.CloseAsync(cancellationToken).ConfigureAwait(false);
            await CancelAndAwaitBackupCallbackIfAny();
        }

        void IStateProviderReplica.Abort()
        {
            storeReplica.Abort();
            CancelAndAwaitBackupCallbackIfAny().ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted);
        }

        Task IStateProviderReplica.BackupAsync(Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
        {
            return ((IStateProviderReplica) this).BackupAsync(BackupOption.Full, Timeout.InfiniteTimeSpan,
                CancellationToken.None, backupCallback);
        }

        Task IStateProviderReplica.BackupAsync(BackupOption option, TimeSpan timeout,
            CancellationToken cancellationToken, Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
        {
            AcquireBackupLock();
            try
            {
                var backupFolderPath = GetLocalBackupFolderPath();
                PrepareBackupFolder(backupFolderPath);
                return storeReplica.BackupAsync(backupFolderPath,
                    option == BackupOption.Full ? StoreBackupOption.Full : StoreBackupOption.Incremental,
                    info => UserBackupCallbackHandler(info, backupCallback), cancellationToken);
            }
            finally
            {
                ReleaseBackupLock();
            }
        }

        Task IStateProviderReplica.RestoreAsync(string backupFolderPath)
        {
            return storeReplica.RestoreAsync(backupFolderPath, restoreSettings, CancellationToken.None);
        }

        Task IStateProviderReplica.RestoreAsync(string backupFolderPath, RestorePolicy restorePolicy,
            CancellationToken cancellationToken)
        {
            return storeReplica.RestoreAsync(backupFolderPath, restoreSettings, cancellationToken);
        }

        void OnCopyComplete(KeyValueStoreEnumerator enumerator)
        {
            //IEnumerator<KeyValueStoreItem> enumerator1 = enumerator.Enumerate("Timestamp_VLTM");
            //while (enumerator1.MoveNext())
            //{
            //    KeyValueStoreItem current = enumerator1.Current;
            //    this.TryDeserializeAndApplyLogicalTimestamp(current.Metadata.Key, current.Value);
            //}
        }

        void OnReplicationOperation(IEnumerator<KeyValueStoreNotification> notification)
        {
            //while (notification.MoveNext())
            //{
            //    KeyValueStoreNotification current = notification.Current;
            //    this.TryDeserializeAndApplyLogicalTimestamp(current.Metadata.Key, current.Value);
            //}
        }

        void OnConfigurationPackageModified(object sender, PackageModifiedEventArgs<ConfigurationPackage> e)
        {
            try
            {
                storeReplica.UpdateReplicatorSettings(BuildReplicatorSettings());
            }
            catch (FabricElementNotFoundException ex)
            {
                WriteError("FabricElementNotFoundException while loading replicator settings from configuation.", ex);
                partition.ReportFault(FaultType.Transient);
            }
            catch (FabricException ex)
            {
                WriteError("FabricException while loading replicator security settings from configuation.", ex);
                partition.ReportFault(FaultType.Transient);
            }
            catch (ArgumentException ex)
            {
                WriteWarning("ArgumentException while updating replicator settings from configuation.", ex);
                partition.ReportFault(FaultType.Transient);
            }
        }

        static void PrepareBackupFolder(string backupFolder)
        {
            try
            {
                InternalFabric.FabricDirectory_Delete(backupFolder, true);
            }
            catch (DirectoryNotFoundException)
            {
            }

            InternalFabric.FabricDirectory_CreateDirectory(backupFolder);
        }

        async Task<bool> UserBackupCallbackHandler(StoreBackupInfo storeBackupInfo,
            Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
        {
            var backupInfo = new BackupInfo(storeBackupInfo.BackupFolder,
                storeBackupInfo.BackupOption == StoreBackupOption.Full ? BackupOption.Full : BackupOption.Incremental,
                BackupInfo.BackupVersion.InvalidBackupVersion);
            await backupCallbackLock.WaitAsync();
            try
            {
                if (isClosingOrAborting)
                    throw new FabricObjectClosedException();
                backupCallbackCts = new CancellationTokenSource();
                backupCallbackTask = backupCallback(backupInfo, backupCallbackCts.Token);
            }
            catch (Exception)
            {
                backupCallbackCts = null;
                backupCallbackTask = null;
                throw;
            }
            finally
            {
                backupCallbackLock.Release();
            }
            return await backupCallbackTask;
        }

        string GetLocalBackupFolderPath()
        {
            return Path.Combine(initParams.CodePackageActivationContext.WorkDirectory,
                "kvsp_" + initParams.PartitionId, initParams.ReplicaId.ToString(), "B");
        }

        void CleanupBackupFolder()
        {
            try
            {
                InternalFabric.FabricDirectory_Delete(GetLocalBackupFolderPath(), true);
            }
            catch (DirectoryNotFoundException)
            {
            }
            catch (Exception ex)
            {
                WriteWarning($"CleanupBackupFolder() failed with: {ex}.");
            }
        }

        async Task CancelAndAwaitBackupCallbackIfAny()
        {
            await backupCallbackLock.WaitAsync();
            isClosingOrAborting = true;
            backupCallbackLock.Release();
            try
            {
                if (backupCallbackCts != null && !backupCallbackCts.IsCancellationRequested)
                    backupCallbackCts.Cancel();
                await AwaitBackupCallbackWithHealthReporting();
            }
            finally
            {
                CleanupBackupFolder();
            }
        }

        void AcquireBackupLock()
        {
            if (Interlocked.CompareExchange(ref isBackupInProgress, 1, 0) == 1)
                throw new FabricBackupInProgressException();
            WriteInfo("Acquired backup lock.");
        }

        void ReleaseBackupLock()
        {
            Volatile.Write(ref isBackupInProgress, 0);
            WriteInfo("Released backup lock.");
        }

        async Task AwaitBackupCallbackWithHealthReporting()
        {
            if (backupCallbackTask == null)
                return;
            CancellationTokenSource delayTaskCts;
            Task delayTask;
            while (true)
            {
                delayTaskCts = new CancellationTokenSource();
                delayTask = Task.Delay(backupCallbackExpectedCancellationTimeSpan, delayTaskCts.Token);
                var finishedTask = await Task.WhenAny(backupCallbackTask as Task, delayTask);
                if (finishedTask != backupCallbackTask)
                    ReportBackupCallbackSlowCancellationHealth();
                else
                    break;
            }
            delayTaskCts.Cancel();
            delayTask.ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted);
        }

        void ReportBackupCallbackSlowCancellationHealth()
        {
            var str =
                $"BackupCallback is taking longer than expected time ({backupCallbackExpectedCancellationTimeSpan.TotalSeconds}s) to cancel.";
            ReportPartitionHealth(new HealthInformation("KvsActorStateProvider", "BackupCallbackSlowCancellation",
                HealthState.Warning)
            {
                TimeToLive = healthInformationTimeToLive,
                RemoveWhenExpired = true,
                Description = str
            });
        }

        void ReportPartitionHealth(HealthInformation healthInformation)
        {
            try
            {
                partition.ReportPartitionHealth(healthInformation);
            }
            catch (Exception ex)
            {
                WriteWarning(
                    $"ReportPartitionHealth() failed with: {ex} while reporting health information: {healthInformation}.");
            }
        }

        ReplicatorSettings GetReplicatorSettings()
        {
            if (userDefinedReplicatorSettings != null)
                return userDefinedReplicatorSettings;
            initParams.CodePackageActivationContext.ConfigurationPackageModifiedEvent += OnConfigurationPackageModified;
            return BuildReplicatorSettings();
        }

        ReplicatorSettings BuildReplicatorSettings()
        {
            var codePackage = initParams.CodePackageActivationContext;
            var replicatorSettings = ReplicatorSettings.LoadFrom(codePackage,
                "Config",
                "ReplicatorConfig");

            replicatorSettings.SecurityCredentials = SecurityCredentials.LoadFrom(codePackage,
                "Config",
                "ReplicatorSecurityConfig");

            var nodeContext = FabricRuntime.GetNodeContext();
            var endpoint = codePackage.GetEndpoint("ReplicatorEndpoint");
            replicatorSettings.ReplicatorAddress = string.Format(CultureInfo.InvariantCulture, "{0}:{1}",
                nodeContext.IPAddressOrFQDN, endpoint.Port);
            return replicatorSettings;
        }

        LocalStoreSettings GetLocalStoreSettings()
        {
            var localStoreSettings = userDefinedLocalStoreSettings;
            var packageActivationContext = initParams.CodePackageActivationContext;

            if (localStoreSettings == null)
            {
                const string configSectionName = "LocalStoreConfig";
                if (packageActivationContext.GetConfigurationPackageObject("Config")
                    .Settings.Sections.Contains(configSectionName))
                    localStoreSettings = LocalEseStoreSettings.LoadFrom(packageActivationContext,
                        "Config",
                        configSectionName);
            }

            if (localStoreSettings == null)
                localStoreSettings = new LocalEseStoreSettings
                {
                    MaxAsyncCommitDelay = TimeSpan.FromMilliseconds(100.0),
                    MaxVerPages = 32768,
                    EnableIncrementalBackup = userDefinedEnableIncrementalBackup
                };
            var eseStoreSettings = localStoreSettings as LocalEseStoreSettings;
            if (eseStoreSettings != null && string.IsNullOrEmpty(eseStoreSettings.DbFolderPath))
                eseStoreSettings.DbFolderPath = packageActivationContext.WorkDirectory;
            return localStoreSettings;
        }

        KeyValueStoreReplicaSettings GetKvsReplicaSettings()
        {
            var storeReplicaSettings = new KeyValueStoreReplicaSettings
            {
                SecondaryNotificationMode = KeyValueStoreReplica.SecondaryNotificationMode.NonBlockingQuorumAcked
            };
            if (userDefinedLogTruncationInterval.HasValue)
                storeReplicaSettings.LogTruncationIntervalInMinutes = userDefinedLogTruncationInterval.Value;
            return storeReplicaSettings;
        }

        KeyValueStoreWrapper CreateStoreReplica()
        {
            return new KeyValueStoreWrapper("KeyValueStateProvider", GetLocalStoreSettings(), GetReplicatorSettings(),
                GetKvsReplicaSettings(), OnCopyComplete, OnReplicationOperation, onDataLossAsyncFunction);
        }

        static void WriteWarning(string warning, Exception ex = null)
        {
            //nameof(KeyValueStateProvider);
            //traceId
        }

        static void WriteInfo(string warning, Exception ex = null)
        {
            //nameof(KeyValueStateProvider);
            //traceId
        }

        static void WriteError(string error, Exception ex = null)
        {
            //nameof(KeyValueStateProvider);
            //traceId
        }

        class KeyValueStoreWrapper : KeyValueStoreReplica
        {
            readonly Action<KeyValueStoreEnumerator> copyHandler;
            readonly Func<CancellationToken, Task<bool>> onDataLossCallback;
            readonly Action<IEnumerator<KeyValueStoreNotification>> replicationHandler;

            public KeyValueStoreWrapper(string storeName, LocalStoreSettings storeSettings,
                ReplicatorSettings replicatorSettings, KeyValueStoreReplicaSettings kvsReplicaSettings,
                Action<KeyValueStoreEnumerator> copyHandler,
                Action<IEnumerator<KeyValueStoreNotification>> replicationHandler,
                Func<CancellationToken, Task<bool>> onDataLossCallback)
                : base(storeName, storeSettings, replicatorSettings, kvsReplicaSettings)
            {
                this.copyHandler = copyHandler;
                this.replicationHandler = replicationHandler;
                this.onDataLossCallback = onDataLossCallback;
            }

            protected override void OnCopyComplete(KeyValueStoreEnumerator enumerator)
            {
                copyHandler(enumerator);
            }

            protected override void OnReplicationOperation(IEnumerator<KeyValueStoreNotification> enumerator)
            {
                replicationHandler(enumerator);
            }

            protected override Task<bool> OnDataLossAsync(CancellationToken cancellationToken)
            {
                return onDataLossCallback(cancellationToken);
            }
        }
    }
}