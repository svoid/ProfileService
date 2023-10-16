-- local Madwork = _G.Madwork
--[[
{Madwork}

-[ProfileService]---------------------------------------
	(STANDALONE VERSION)
	DataStore profiles - universal session-locked savable table API
	
	Official documentation:
		https://madstudioroblox.github.io/ProfileService/

	DevForum discussion:
		https://devforum.roblox.com/t/ProfileService/667805
	
	WARNINGS FOR "Profile.Data" VALUES:
	 	! Do not create numeric tables with gaps - attempting to replicate such tables will result in an error;
		! Do not create mixed tables (some values indexed by number and others by string key), as only
		     the data indexed by number will be replicated.
		! Do not index tables by anything other than numbers and strings.
		! Do not reference Roblox Instances
		! Do not reference userdata (Vector3, Color3, CFrame...) - Serialize userdata before referencing
		! Do not reference functions
		
	WARNING: Calling ProfileStore:LoadProfileAsync() with a "profileKey" which wasn't released in the SAME SESSION will result
		in an error! If you want to "ProfileStore:LoadProfileAsync()" instead of using the already loaded profile, :Release()
		the old Profile object.
		
	Members:
	
		ProfileService.ServiceLocked         [bool]
		
		ProfileService.IssueSignal           [ScriptSignal] (errorMessage, profileStoreName, profileKey)
		ProfileService.CorruptionSignal      [ScriptSignal] (profileStoreName, profileKey)
		ProfileService.CriticalStateSignal   [ScriptSignal] (is_critical_state)
	
	Functions:
	
		ProfileService.GetProfileStore(profileStoreIndex, profileTemplate) --> [ProfileStore]
			profileStoreIndex   [string] -- DataStore name
			OR
			profileStoreIndex   [table]: -- Allows the developer to define more GlobalDataStore variables
				{
					Name = "StoreName", -- [string] -- DataStore name
					-- Optional arguments:
					Scope = "StoreScope", -- [string] -- DataStore scope
				}
			profileTemplate      [table] -- Profiles will default to given table (hard-copy) when no data was saved previously

		ProfileService.IsLive() --> [bool] -- (CAN YIELD!!!)
			-- Returns true if ProfileService is connected to live Roblox DataStores
				
	Members [ProfileStore]:
	
		ProfileStore.Mock   [ProfileStore] -- Reflection of ProfileStore methods, but the methods will use a mock DataStore
		
	Methods [ProfileStore]:
	
		ProfileStore:LoadProfileAsync(profileKey, notReleasedHandler) --> [Profile] or nil -- notReleasedHandler(placeId, gameJobId)
			profileKey            [string] -- DataStore key
			notReleasedHandler   nil or []: -- Defaults to "ForceLoad"
				[string] "ForceLoad" -- Force loads profile on first call
				OR
				[string] "Steal" -- Steals the profile ignoring it's session lock
				OR
				[function] (placeId, gameJobId) --> [string] "Repeat", "Cancel", "ForceLoad" or "Steal"
					placeId      [number] or nil
					gameJobId   [string] or nil

				-- notReleasedHandler [function] will be triggered in cases where the profile is not released by a session. This
				--	function may yield for as long as desirable and must return one of three string values:

						["Repeat"] - ProfileService will repeat the profile loading proccess and may trigger the release handler again
						["Cancel"] - ProfileStore:LoadProfileAsync() will immediately return nil
						["ForceLoad"] - ProfileService will repeat the profile loading call, but will return Profile object afterwards
							and release the profile for another session that has loaded the profile
						["Steal"] - The profile will usually be loaded immediately, ignoring an existing remote session lock and applying
							a session lock for this session.

		ProfileStore:GlobalUpdateProfileAsync(profileKey, updateHandler) --> [GlobalUpdates] or nil
			-- Returns GlobalUpdates object if update was successful, otherwise returns nil
			profileKey      [string] -- DataStore key
			updateHandler   [function] (global_updates [GlobalUpdates])
			
		ProfileStore:ViewProfileAsync(profileKey, version) --> [Profile] or nil
			-- Reads profile without requesting a session lock; Data will not be saved and profile doesn't need to be released
			profileKey   [string] -- DataStore key
			version       nil or [string] -- DataStore key version

		ProfileStore:ProfileVersionQuery(profileKey, sortDirection, minDate, maxDate) --> [ProfileVersionQuery]
			profileKey      [string]
			sortDirection   nil or [Enum.SortDirection]
			minDate         nil or [DateTime]
			maxDate         nil or [DateTime]
			
		ProfileStore:WipeProfileAsync(profileKey) --> is_wipe_successful [bool]
			-- Completely wipes out profile data from the DataStore / mock DataStore with no way to recover it.
						
		* Parameter description for "ProfileStore:GlobalUpdateProfileAsync()":
		
			profileKey      [string] -- DataStore key
			updateHandler   [function] (GlobalUpdates) -- This function gains access to GlobalUpdates object methods
				(updateHandler can't yield)

	Methods [ProfileVersionQuery]:

		ProfileVersionQuery:NextAsync() --> [Profile] or nil -- (Yields)
			-- Returned profile has the same rules as profile returned by :ViewProfileAsync()
		
	Members [Profile]:
	
		Profile.Data              [table] -- Writable table that gets saved automatically and once the profile is released
		Profile.MetaData          [table] (Read-only) -- Information about this profile
		
			Profile.MetaData.ProfileCreateTime   [number] (Read-only) -- os.time() timestamp of profile creation
			Profile.MetaData.SessionLoadCount    [number] (Read-only) -- Amount of times the profile was loaded
			Profile.MetaData.ActiveSession       [table] (Read-only) {placeId, gameJobId} / nil -- Set to a session link if a
				game session is currently having this profile loaded; nil if released
			Profile.MetaData.MetaTags            [table] {["tagName"] = tag_value, ...} -- Saved and auto-saved just like Profile.Data
			Profile.MetaData.MetaTagsLatest      [table] (Read-only) -- Latest version of MetaData.MetaTags that was definetly saved to DataStore
				(You can use Profile.MetaData.MetaTagsLatest for product purchase save confirmation, but create a system to clear old tags after
				they pile up)

		Profile.MetaTagsUpdated   [ScriptSignal] (meta_tags_latest) -- Fires after every auto-save, after
			--	Profile.MetaData.MetaTagsLatest has been updated with the version that's guaranteed to be saved;
			--  .MetaTagsUpdated will fire regardless of whether .MetaTagsLatest changed after update;
			--	.MetaTagsUpdated may fire after the Profile is released - changes to Profile.Data are not saved
			--	after release.

		Profile.RobloxMetaData    [table] -- Writable table that gets saved automatically and once the profile is released
		Profile.UserIds           [table] -- (Read-only) -- {userId [number], ...} -- User ids associated with this profile

		Profile.KeyInfo           [DataStoreKeyInfo]
		Profile.KeyInfoUpdated    [ScriptSignal] (keyInfo [DataStoreKeyInfo])
		
		Profile.GlobalUpdates     [GlobalUpdates]
		
	Methods [Profile]:
	
		-- SAFE METHODS - Will not error after profile expires:
		Profile:IsActive() --> [bool] -- Returns true while the profile is active and can be written to
			
		Profile:GetMetaTag(tagName) --> value [any]
			tagName   [string]
		
		Profile:Reconcile() -- Fills in missing (nil) [string_key] = [value] pairs to the Profile.Data structure
		
		Profile:ListenToRelease(listener) --> [ScriptConnection] (placeId / nil, gameJobId / nil)
			-- WARNING: Profiles can be released externally if another session force-loads
			--	this profile - use :ListenToRelease() to handle player leaving cleanup.
			
		Profile:Release() -- Call after the session has finished working with this profile
			e.g., after the player leaves (Profile object will become expired) (Does not yield)

		Profile:ListenToHopReady(listener) --> [ScriptConnection] () -- Passed listener will be executed after the releasing UpdateAsync call finishes;
			--	Wrap universe teleport requests with this method AFTER releasing the profile to improve session lock sharing between universe places;
			--  :ListenToHopReady() will usually call the listener in around a second, but may ocassionally take up to 7 seconds when a release happens
			--	next to an auto-update in regular usage scenarios.

		Profile:AddUserId(userId) -- Associates userId with profile (GDPR compliance)
			userId   [number]

		Profile:RemoveUserId(userId) -- Unassociates userId with profile (safe function)
			userId   [number]

		Profile:Identify() --> [string] -- Returns a string containing DataStore name, scope and key; Used for debug;
			-- Example return: "[Store:"GameData";Scope:"Live";Key:"Player_2312310"]"
		
		Profile:SetMetaTag(tagName, value) -- Equivalent of Profile.MetaData.MetaTags[tagName] = value
			tagName   [string]
			value      [any]
		
		Profile:Save() -- Call to quickly progress global update state or to speed up save validation processes (Does not yield)

		-- VIEW-MODE ONLY:

		Profile:ClearGlobalUpdates() -- Clears all global updates data from a profile payload

		Profile:OverwriteAsync() -- (Yields) Saves the profile payload to the DataStore and removes the session lock
		
	Methods [GlobalUpdates]:
	
	-- ALWAYS PUBLIC:
		GlobalUpdates:GetActiveUpdates() --> [table] {{updateId, updateData [table]}, ...}
		GlobalUpdates:GetLockedUpdates() --> [table] {{updateId, updateData [table]}, ...}
		
	-- ONLY WHEN FROM "Profile.GlobalUpdates":
		GlobalUpdates:ListenToNewActiveUpdate(listener) --> [ScriptConnection] (updateId, updateData)
			updateData   [table]
		GlobalUpdates:ListenToNewLockedUpdate(listener) --> [ScriptConnection] (updateId, updateData)
			updateData   [table]
		GlobalUpdates:LockActiveUpdate(updateId)  -- WARNING: will error after profile expires
		GlobalUpdates:ClearLockedUpdate(updateId) -- WARNING: will error after profile expires
		
	-- EXPOSED TO "updateHandler" DURING ProfileStore:GlobalUpdateProfileAsync() CALL
		GlobalUpdates:AddActiveUpdate(updateData)
			updateData   [table]
		GlobalUpdates:ChangeActiveUpdate(updateId, updateData)
			updateData   [table]
		GlobalUpdates:ClearActiveUpdate(updateId)
		
--]]

local SETTINGS = {

	AutoSaveProfiles = 30, -- Seconds (This value may vary - ProfileService will split the auto save load evenly in the given time)
	RobloxWriteCooldown = 7, -- Seconds between successive DataStore calls for the same key
	ForceLoadMaxSteps = 8, -- Steps taken before ForceLoad request steals the active session for a profile
	AssumeDeadSessionLock = 30 * 60, -- (seconds) If a profile hasn't been updated for 30 minutes, assume the session lock is dead
	-- As of writing, os.time() is not completely reliable, so we can only assume session locks are dead after a significant amount of time.

	IssueCountForCriticalState = 5, -- Issues to collect to announce critical state
	IssueLast = 120, -- Seconds
	CriticalStateLast = 120, -- Seconds

	MetaTagsUpdatedValues = { -- Technical stuff - do not alter
		ProfileCreateTime = true,
		SessionLoadCount = true,
		ActiveSession = true,
		ForceLoadSession = true,
		LastUpdate = true,
	},

}

local Madwork -- Standalone Madwork reference for portable version of ProfileService
do

	local MadworkScriptSignal = {}

	local FreeRunnerThread = nil

	local function AcquireRunnerThreadAndCallEventHandler(fn, ...)
		local acquired_runner_thread = FreeRunnerThread
		FreeRunnerThread = nil
		fn(...)
		FreeRunnerThread = acquired_runner_thread
	end

	local function RunEventHandlerInFreeThread(...)
		AcquireRunnerThreadAndCallEventHandler(...)
		while true do
			AcquireRunnerThreadAndCallEventHandler(coroutine.yield())
		end
	end

	-- ScriptConnection object:

	local ScriptConnection = {
		--[[
			_Listener = listener,
			_ScriptSignal = script_signal,
			_DisconnectListener = disconnectListener,
			_DisconnectParam = disconnectParam,
			
			_Next = next_script_connection,
			_IsConnected = is_connected,
		--]]
	}
	ScriptConnection.__index = ScriptConnection

	function ScriptConnection:Disconnect()

		if self._IsConnected == false then
			return
		end

		self._IsConnected = false
		self._ScriptSignal._ListenerCount -= 1

		if self._ScriptSignal._Head == self then
			self._ScriptSignal._Head = self._Next
		else
			local prev = self._ScriptSignal._Head
			while prev ~= nil and prev._Next ~= self do
				prev = prev._Next
			end
			if prev ~= nil then
				prev._Next = self._Next
			end
		end

		if self._DisconnectListener ~= nil then
			if not FreeRunnerThread then
				FreeRunnerThread = coroutine.create(RunEventHandlerInFreeThread)
			end
			task.spawn(FreeRunnerThread, self._DisconnectListener, self._DisconnectParam)
			self._DisconnectListener = nil
		end

	end

	-- ScriptSignal object:

	local ScriptSignal = {
		--[[
			_Head = nil,
			_ListenerCount = 0,
		--]]
	}
	ScriptSignal.__index = ScriptSignal

	function ScriptSignal:Connect(listener, disconnectListener, disconnectParam) --> [ScriptConnection]

		local scriptConnection = {
			_Listener = listener,
			_ScriptSignal = self,
			_DisconnectListener = disconnectListener,
			_DisconnectParam = disconnectParam,

			_Next = self._Head,
			_IsConnected = true,
		}
		setmetatable(scriptConnection, ScriptConnection)

		self._Head = scriptConnection
		self._ListenerCount += 1

		return scriptConnection

	end

	function ScriptSignal:GetListenerCount() --> [number]
		return self._ListenerCount
	end

	function ScriptSignal:Fire(...)
		local item = self._Head
		while item ~= nil do
			if item._IsConnected == true then
				if not FreeRunnerThread then
					FreeRunnerThread = coroutine.create(RunEventHandlerInFreeThread)
				end
				task.spawn(FreeRunnerThread, item._Listener, ...)
			end
			item = item._Next
		end
	end

	function ScriptSignal:FireUntil(continue_callback, ...)
		local item = self._Head
		while item ~= nil do
			if item._IsConnected == true then
				item._Listener(...)
				if continue_callback() ~= true then
					return
				end
			end
			item = item._Next
		end
	end

	function MadworkScriptSignal.NewScriptSignal() --> [ScriptSignal]
		return {
			_Head = nil,
			_ListenerCount = 0,
			Connect = ScriptSignal.Connect,
			GetListenerCount = ScriptSignal.GetListenerCount,
			Fire = ScriptSignal.Fire,
			FireUntil = ScriptSignal.FireUntil,
		}
	end

	-- Madwork framework namespace:

	Madwork = {
		NewScriptSignal = MadworkScriptSignal.NewScriptSignal,
		ConnectToOnClose = function(task, runInStudioMode)
			if game:GetService("RunService"):IsStudio() == false or runInStudioMode == true then
				game:BindToClose(task)
			end
		end,
	}

end

----- Service Table -----

local ProfileService = {

	ServiceLocked = false, -- Set to true once the server is shutting down

	IssueSignal = Madwork.NewScriptSignal(), -- (errorMessage, profileStoreName, profileKey) -- Fired when a DataStore API call throws an error
	CorruptionSignal = Madwork.NewScriptSignal(), -- (profileStoreName, profileKey) -- Fired when DataStore key returns a value that has
	-- all or some of it's profile components set to invalid data types. E.g., accidentally setting Profile.Data to a noon table value

	CriticalState = false, -- Set to true while DataStore service is throwing too many errors
	CriticalStateSignal = Madwork.NewScriptSignal(), -- (is_critical_state) -- Fired when CriticalState is set to true
	-- (You may alert players with this, or set up analytics)

	ServiceIssueCount = 0,

	_ActiveProfileStores = {}, -- {profileStore, ...}

	_AutoSaveList = {}, -- {profile, ...} -- loaded profile table which will be circularly auto-saved

	_IssueQueue = {}, -- [table] {issueTime, ...}
	--TODO: unused
	_CriticalStateStart = 0, -- [number] 0 = no critical state / os.clock() = critical state start

	-- Debug:
	_MockDataStore = {},
	_UserMockDataStore = {},

	_UseMockDataStore = false,

}

--[[
	Saved profile structure:
	
	DataStoreProfile = {
		Data = {},
		MetaData = {
			ProfileCreateTime = 0,
			SessionLoadCount = 0,
			ActiveSession = {placeId, gameJobId} / nil,
			ForceLoadSession = {placeId, gameJobId} / nil,
			MetaTags = {},
			LastUpdate = 0, -- os.time()
		},
		RobloxMetaData = {},
		UserIds = {},
		GlobalUpdates = {
			updateIndex,
			{
				{updateId, versionId, updateLocked, updateData},
				...
			}
		},
	}
	
	OR
	
	DataStoreProfile = {
		GlobalUpdates = {
			updateIndex,
			{
				{updateId, versionId, updateLocked, updateData},
				...
			}
		},
	}
--]]

----- Private Variables -----

local ActiveProfileStores = ProfileService._ActiveProfileStores
local AutoSaveList = ProfileService._AutoSaveList
local IssueQueue = ProfileService._IssueQueue

local DataStoreService = game:GetService("DataStoreService")
local RunService = game:GetService("RunService")

local PlaceId = game.PlaceId
local JobId = game.JobId

local AutoSaveIndex = 1 -- Next profile to auto save
local LastAutoSave = os.clock()

local LoadIndex = 0

local ActiveProfileLoadJobs = 0 -- Number of active threads that are loading in profiles
local ActiveProfileSaveJobs = 0 -- Number of active threads that are saving profiles

local CriticalStateStart = 0 -- os.clock()

local IsStudio = RunService:IsStudio()
local IsLiveCheckActive = false

local UseMockDataStore = false
local MockDataStore = ProfileService._MockDataStore -- Mock data store used when API access is disabled

local UserMockDataStore = ProfileService._UserMockDataStore -- Separate mock data store accessed via ProfileStore.Mock
local UseMockTag = {}

local CustomWriteQueue = {
	--[[
		[store] = {
			[key] = {
				LastWrite = os.clock(),
				Queue = {callback, ...},
				CleanupJob = nil,
			},
			...
		},
		...
	--]]
}

----- Utils -----

local function DeepCopyTable(t)
	local copy = {}
	for key, value in pairs(t) do
		if type(value) == "table" then
			copy[key] = DeepCopyTable(value)
		else
			copy[key] = value
		end
	end
	return copy
end

local function ReconcileTable(target, template)
	for k, v in pairs(template) do
		if type(k) == "string" then -- Only string keys will be reconciled
			if target[k] == nil then
				if type(v) == "table" then
					target[k] = DeepCopyTable(v)
				else
					target[k] = v
				end
			elseif type(target[k]) == "table" and type(v) == "table" then
				ReconcileTable(target[k], v)
			end
		end
	end
end

----- Private functions -----

local function IdentifyProfile(storeName, storeScope, key)
	return string.format(
		"[Store:\"%s\";%sKey:\"%s\"]",
		storeName,
		storeScope ~= nil and string.format("Scope:\"%s\";", storeScope) or "",
		key
	)
end

local function CustomWriteQueueCleanup(store, key)
	if CustomWriteQueue[store] ~= nil then
		CustomWriteQueue[store][key] = nil
		if next(CustomWriteQueue[store]) == nil then
			CustomWriteQueue[store] = nil
		end
	end
end

local function CustomWriteQueueMarkForCleanup(store, key)
	if CustomWriteQueue[store] ~= nil then
		if CustomWriteQueue[store][key] ~= nil then

			local queueData = CustomWriteQueue[store][key]
			local queue = queueData.Queue

			if queueData.CleanupJob == nil then

				queueData.CleanupJob = RunService.Heartbeat:Connect(function()
					if os.clock() - queueData.LastWrite > SETTINGS.RobloxWriteCooldown and #queue == 0 then
						queueData.CleanupJob:Disconnect()
						CustomWriteQueueCleanup(store, key)
					end
				end)

			end

		elseif next(CustomWriteQueue[store]) == nil then
			CustomWriteQueue[store] = nil
		end
	end
end

local function CustomWriteQueueAsync(callback, store, key) --> ... -- Passed return from callback

	if CustomWriteQueue[store] == nil then
		CustomWriteQueue[store] = {}
	end
	if CustomWriteQueue[store][key] == nil then
		CustomWriteQueue[store][key] = {LastWrite = 0, Queue = {}, CleanupJob = nil}
	end

	local queueData = CustomWriteQueue[store][key]
	local queue = queueData.Queue

	-- Cleanup job:
	if queueData.CleanupJob ~= nil then
		queueData.CleanupJob:Disconnect()
		queueData.CleanupJob = nil
	end

	-- Queue logic:
	if os.clock() - queueData.LastWrite > SETTINGS.RobloxWriteCooldown and #queue == 0 then
		queueData.LastWrite = os.clock()
		return callback()
	else
		table.insert(queue, callback)
		while true do
			if os.clock() - queueData.LastWrite > SETTINGS.RobloxWriteCooldown and queue[1] == callback then
				table.remove(queue, 1)
				queueData.LastWrite = os.clock()
				return callback()
			end
			task.wait()
		end
	end

end

local function IsCustomWriteQueueEmptyFor(store, key) --> is_empty [bool]
	local lookup = CustomWriteQueue[store]
	if lookup ~= nil then
		lookup = lookup[key]
		return lookup == nil or #lookup.Queue == 0
	end
	return true
end

local function WaitForLiveAccessCheck() -- This function was created to prevent the ProfileService module yielding execution when required
	while IsLiveCheckActive == true do
		task.wait()
	end
end

local function WaitForPendingProfileStore(profileStore)
	while profileStore._IsPending == true do
		task.wait()
	end
end

local function RegisterIssue(errorMessage, storeName, storeScope, profileKey) -- Called when a DataStore API call errors
	warn("[ProfileService]: DataStore API error " .. IdentifyProfile(storeName, storeScope, profileKey) .. " - \"" .. tostring(errorMessage) .. "\"")
	table.insert(IssueQueue, os.clock()) -- Adding issue time to queue
	ProfileService.IssueSignal:Fire(tostring(errorMessage), storeName, profileKey)
end

local function RegisterCorruption(storeName, storeScope, profileKey) -- Called when a corrupted profile is loaded
	warn("[ProfileService]: Resolved profile corruption " .. IdentifyProfile(storeName, storeScope, profileKey))
	ProfileService.CorruptionSignal:Fire(storeName, profileKey)
end

local function NewMockDataStoreKeyInfo(params)
	local versionIdString = tostring(params.VersionId or 0)
	local metaData = params.MetaData or {}
	local userIds = params.UserIds or {}

	return {
		CreatedTime = params.CreatedTime,
		UpdatedTime = params.UpdatedTime,
		Version = string.rep("0", 16) .. "."
			.. string.rep("0", 10 - string.len(versionIdString)) .. versionIdString
			.. "." .. string.rep("0", 16) .. "." .. "01",

		GetMetadata = function()
			return DeepCopyTable(metaData)
		end,

		GetUserIds = function()
			return DeepCopyTable(userIds)
		end,
	}
end

local function MockUpdateAsync(mockDataStore, profileStoreName, key, transformFunction, isGetCall) --> loadedData, keyInfo

	local profileStore = mockDataStore[profileStoreName]

	if profileStore == nil then
		profileStore = {}
		mockDataStore[profileStoreName] = profileStore
	end

	local epochTime = math.floor(os.time() * 1000)
	local mockEntry = profileStore[key]
	local mockEntryWasNil = false

	if mockEntry == nil then
		mockEntryWasNil = true
		if isGetCall ~= true then
			mockEntry = {
				Data = nil,
				CreatedTime = epochTime,
				UpdatedTime = epochTime,
				VersionId = 0,
				UserIds = {},
				MetaData = {},
			}
			profileStore[key] = mockEntry
		end
	end

	local mockKeyInfo = mockEntryWasNil == false and NewMockDataStoreKeyInfo(mockEntry) or nil

	local transform, userIds, robloxMetaData = transformFunction(mockEntry and mockEntry.Data, mockKeyInfo)

	if transform == nil then
		return nil
	else
		if mockEntry ~= nil and isGetCall ~= true then
			mockEntry.Data = transform
			mockEntry.UserIds = DeepCopyTable(userIds or {})
			mockEntry.MetaData = DeepCopyTable(robloxMetaData or {})
			mockEntry.VersionId += 1
			mockEntry.UpdatedTime = epochTime
		end

		return DeepCopyTable(transform), mockEntry ~= nil and NewMockDataStoreKeyInfo(mockEntry) or nil
	end

end

local function IsThisSession(sessionTag)
	return sessionTag[1] == PlaceId and sessionTag[2] == JobId
end

--[[
updateSettings = {
	ExistingProfileHandle = function(latestData),
	MissingProfileHandle = function(latestData),
	EditProfile = function(lastest_data),
}
--]]
local function StandardProfileUpdateAsyncDataStore(profileStore, profileKey, updateSettings, isUserMock, isGetCall, version) --> loadedData, keyInfo
	local loadedData, keyInfo
	local success, errorMessage = pcall(function()
		local transformFunction = function(latestData)

			local missingProfile = false
			local dataCorrupted = false
			local globalUpdatesData = {0, {}}

			if latestData == nil then
				missingProfile = true
			elseif type(latestData) ~= "table" then
				missingProfile = true
				dataCorrupted = true
			end

			if type(latestData) == "table" then
				-- Case #1: Profile was loaded
				if type(latestData.Data) == "table"
					and type(latestData.MetaData) == "table"
					and type(latestData.GlobalUpdates) == "table" then

					latestData.WasCorrupted = false -- Must be set to false if set previously
					globalUpdatesData = latestData.GlobalUpdates
					if updateSettings.ExistingProfileHandle ~= nil then
						updateSettings.ExistingProfileHandle(latestData)
					end
					-- Case #2: Profile was not loaded but GlobalUpdate data exists
				elseif latestData.Data == nil
					and latestData.MetaData == nil
					and type(latestData.GlobalUpdates) == "table" then

					latestData.WasCorrupted = false -- Must be set to false if set previously
					globalUpdatesData = latestData.GlobalUpdates or globalUpdatesData
					missingProfile = true
				else
					missingProfile = true
					dataCorrupted = true
				end
			end

			-- Case #3: Profile was not created or corrupted and no GlobalUpdate data exists
			if missingProfile == true then
				latestData = {
					-- Data = nil,
					-- MetaData = nil,
					GlobalUpdates = globalUpdatesData,
				}
				if updateSettings.MissingProfileHandle ~= nil then
					updateSettings.MissingProfileHandle(latestData)
				end
			end

			-- Editing profile:
			if updateSettings.EditProfile ~= nil then
				updateSettings.EditProfile(latestData)
			end

			-- Data corruption handling (Silently override with empty profile) (Also run Case #1)
			if dataCorrupted == true then
				latestData.WasCorrupted = true -- Temporary tag that will be removed on first save
			end

			return latestData, latestData.UserIds, latestData.RobloxMetaData
		end
		if isUserMock == true then -- Used when the profile is accessed through ProfileStore.Mock
			loadedData, keyInfo = MockUpdateAsync(UserMockDataStore, profileStore._ProfileStoreLookup, profileKey, transformFunction, isGetCall)
			task.wait() -- Simulate API call yield
		elseif UseMockDataStore == true then -- Used when API access is disabled
			loadedData, keyInfo = MockUpdateAsync(MockDataStore, profileStore._ProfileStoreLookup, profileKey, transformFunction, isGetCall)
			task.wait() -- Simulate API call yield
		else
			loadedData, keyInfo = CustomWriteQueueAsync(
				function() -- Callback
					if isGetCall == true then
						local getData, getKeyInfo
						if version ~= nil then
							local success, errorMessage = pcall(function()
								getData, getKeyInfo = profileStore._GlobalDataStore:GetVersionAsync(profileKey, version)
							end)
							if success == false and type(errorMessage) == "string" and string.find(errorMessage, "not valid") ~= nil then
								warn("[ProfileService]: Passed version argument is not valid; Traceback:\n" .. debug.traceback())
							end
						else
							getData, getKeyInfo = profileStore._GlobalDataStore:GetAsync(profileKey)
						end
						getData = transformFunction(getData)
						return getData, getKeyInfo
					else
						return profileStore._GlobalDataStore:UpdateAsync(profileKey, transformFunction)
					end
				end,
				profileStore._ProfileStoreLookup, -- Store
				profileKey -- Key
			)
		end
	end)
	if success == true and type(loadedData) == "table" then
		-- Corruption handling:
		if loadedData.WasCorrupted == true and isGetCall ~= true then
			RegisterCorruption(
				profileStore._ProfileStoreName,
				profileStore._ProfileStoreScope,
				profileKey
			)
		end
		-- Return loadedData:
		return loadedData, keyInfo
	else
		RegisterIssue(
			(errorMessage ~= nil) and errorMessage or "Undefined error",
			profileStore._ProfileStoreName,
			profileStore._ProfileStoreScope,
			profileKey
		)
		-- Return nothing:
		return nil
	end
end

local function RemoveProfileFromAutoSave(profile)
	local autoSaveIndex = table.find(AutoSaveList, profile)
	if autoSaveIndex ~= nil then
		table.remove(AutoSaveList, autoSaveIndex)
		if autoSaveIndex < AutoSaveIndex then
			AutoSaveIndex -= 1 -- Table contents were moved left before AutoSaveIndex so move AutoSaveIndex left as well
		end
		if AutoSaveList[AutoSaveIndex] == nil then -- AutoSaveIndex was at the end of the AutoSaveList - reset to 1
			AutoSaveIndex = 1
		end
	end
end

local function AddProfileToAutoSave(profile) -- Notice: Makes sure this profile isn't auto-saved too soon
	-- Add at AutoSaveIndex and move AutoSaveIndex right:
	table.insert(AutoSaveList, AutoSaveIndex, profile)
	if #AutoSaveList > 1 then
		AutoSaveIndex += 1
	elseif #AutoSaveList == 1 then
		-- First profile created - make sure it doesn't get immediately auto saved:
		LastAutoSave = os.clock()
	end
end

local function ReleaseProfileInternally(profile)
	-- 1) Remove profile object from ProfileService references: --
	-- Clear reference in ProfileStore:
	local profileStore = profile._ProfileStore
	local loadedProfiles = profile._IsUserMock == true and profileStore._MockLoadedProfiles or profileStore._LoadedProfiles
	loadedProfiles[profile._ProfileKey] = nil
	
	if next(profileStore._LoadedProfiles) == nil and next(profileStore._MockLoadedProfiles) == nil then -- ProfileStore has turned inactive
		local index = table.find(ActiveProfileStores, profileStore)
		if index ~= nil then
			table.remove(ActiveProfileStores, index)
		end
	end

	-- Clear auto update reference:
	RemoveProfileFromAutoSave(profile)
	-- 2) Trigger release listeners: --
	local placeId
	local gameJobId
	local activeSession = profile.MetaData.ActiveSession

	if activeSession ~= nil then
		placeId = activeSession[1]
		gameJobId = activeSession[2]
	end

	profile._ReleaseListeners:Fire(placeId, gameJobId)
end

local function CheckForNewGlobalUpdates(profile, oldGlobalUpdatesData, newGlobalUpdatesData)
	local globalUpdatesObject = profile.GlobalUpdates -- [GlobalUpdates]
	local pendingUpdateLock = globalUpdatesObject._PendingUpdateLock -- {updateId, ...}
	local pendingUpdateClear = globalUpdatesObject._PendingUpdateClear -- {updateId, ...}
	
	-- "old_" or "new_" globalUpdatesData = {updateIndex, {{updateId, versionId, updateLocked, updateData}, ...}}
	for _, newGlobalUpdate in ipairs(newGlobalUpdatesData[2]) do
		
		-- Find old global update with the same updateId:
		local oldGlobalUpdate
		for _, globalUpdate in ipairs(oldGlobalUpdatesData[2]) do
			if globalUpdate[1] == newGlobalUpdate[1] then
				oldGlobalUpdate = globalUpdate
				break
			end
		end

		-- A global update is new when it didn't exist before or its versionId or updateLocked state changed:
		local isNew = false
		if oldGlobalUpdate == nil or newGlobalUpdate[2] > oldGlobalUpdate[2] or newGlobalUpdate[3] ~= oldGlobalUpdate[3] then
			isNew = true
		end
		if isNew == true then
			-- Active global updates:
			if newGlobalUpdate[3] == false then
				-- Check if update is not pending to be locked: (Preventing firing new active update listeners more than necessary)
				local isPendingLock = false
				
				for _, updateId in ipairs(pendingUpdateLock) do
					if newGlobalUpdate[1] == updateId then
						isPendingLock = true
						break
					end
				end

				if isPendingLock == false then
					-- Trigger new active update listeners:
					globalUpdatesObject._NewActiveUpdateListeners:Fire(newGlobalUpdate[1], newGlobalUpdate[4])
				end

			end
			-- Locked global updates:
			if newGlobalUpdate[3] == true then
				-- Check if update is not pending to be cleared: (Preventing firing new locked update listeners after marking a locked update for clearing)
				local isPendingClear = false
				
				for _, updateId in ipairs(pendingUpdateClear) do
					if newGlobalUpdate[1] == updateId then
						isPendingClear = true
						break
					end
				end

				if isPendingClear == false then
					-- Trigger new locked update listeners:

					globalUpdatesObject._NewLockedUpdateListeners:FireUntil(
						function()
							-- Check if listener marked the update to be cleared:
							-- Normally there should be only one listener per profile for new locked global updates, but
							-- in case several listeners are connected we will not trigger more listeners after one listener
							-- marks the locked global update to be cleared.
							return table.find(pendingUpdateClear, newGlobalUpdate[1]) == nil
						end,
						newGlobalUpdate[1], newGlobalUpdate[4]
					)

				end
			end
		end
	end
end

local function SaveProfileAsync(profile, releaseFromSession, isOverwriting)
	if type(profile.Data) ~= "table" then
		RegisterCorruption(
			profile._ProfileStore._ProfileStoreName,
			profile._ProfileStore._ProfileStoreScope,
			profile._ProfileKey
		)
		error("PROFILE DATA CORRUPTED DURING RUNTIME! Profile: " .. profile:Identify())
	end
	
	if releaseFromSession == true and isOverwriting ~= true then
		ReleaseProfileInternally(profile)
	end

	ActiveProfileSaveJobs += 1
	local lastSessionLoadCount = profile.MetaData.SessionLoadCount
	-- Compare "SessionLoadCount" when writing to profile to prevent a rare case of repeat last save when the profile is loaded on the same server again
	local repeatSaveFlag = true -- Released Profile save calls have to repeat until they succeed
	while repeatSaveFlag == true do
		if releaseFromSession ~= true then
			repeatSaveFlag = false
		end
		local loadedData, keyInfo = StandardProfileUpdateAsyncDataStore(
			profile._ProfileStore,
			profile._ProfileKey,
			{
				ExistingProfileHandle = nil,
				MissingProfileHandle = nil,
				EditProfile = function(latestData)

					local sessionOwnsProfile = false
					local force_load_pending = false

					if isOverwriting ~= true then
						-- 1) Check if this session still owns the profile: --
						local activeSession = latestData.MetaData.ActiveSession
						local forceLoadSession = latestData.MetaData.ForceLoadSession
						local sessionLoadCount = latestData.MetaData.SessionLoadCount

						if type(activeSession) == "table" then
							sessionOwnsProfile = IsThisSession(activeSession) and sessionLoadCount == lastSessionLoadCount
						end
						if type(forceLoadSession) == "table" then
							force_load_pending = not IsThisSession(forceLoadSession)
						end
					else
						sessionOwnsProfile = true
					end

					if sessionOwnsProfile == true then -- We may only edit the profile if this session has ownership of the profile

						if isOverwriting ~= true then
							-- 2) Manage global updates: --
							local latest_global_updates_data = latestData.GlobalUpdates -- {updateIndex, {{updateId, versionId, updateLocked, updateData}, ...}}
							local latestGlobalUpdatesList = latest_global_updates_data[2]

							local globalUpdatesObject = profile.GlobalUpdates -- [GlobalUpdates]
							local pendingUpdateLock = globalUpdatesObject._PendingUpdateLock -- {updateId, ...}
							local pendingUpdateClear = globalUpdatesObject._PendingUpdateClear -- {updateId, ...}
							-- Active update locking:
							for i = 1, #latestGlobalUpdatesList do
								for _, lockId in ipairs(pendingUpdateLock) do
									if latestGlobalUpdatesList[i][1] == lockId then
										latestGlobalUpdatesList[i][3] = true
										break
									end
								end
							end
							-- Locked update clearing:
							for _, clear_id in ipairs(pendingUpdateClear) do
								for i = 1, #latestGlobalUpdatesList do
									if latestGlobalUpdatesList[i][1] == clear_id and latestGlobalUpdatesList[i][3] == true then
										table.remove(latestGlobalUpdatesList, i)
										break
									end
								end
							end
						end

						-- 3) Save profile data: --
						latestData.Data = profile.Data
						latestData.RobloxMetaData = profile.RobloxMetaData
						latestData.UserIds = profile.UserIds

						if isOverwriting ~= true then
							latestData.MetaData.MetaTags = profile.MetaData.MetaTags -- MetaData.MetaTags is the only actively savable component of MetaData
							latestData.MetaData.LastUpdate = os.time()
							if releaseFromSession == true or force_load_pending == true then
								latestData.MetaData.ActiveSession = nil
							end
						else
							latestData.MetaData = profile.MetaData
							latestData.MetaData.ActiveSession = nil
							latestData.MetaData.ForceLoadSession = nil
							latestData.GlobalUpdates = profile.GlobalUpdates._UpdatesLatest
						end

					end
				end,
			},
			profile._IsUserMock
		)

		if loadedData ~= nil and keyInfo ~= nil then
			if isOverwriting == true then break end

			repeatSaveFlag = false
			-- 4) Set latest data in profile: --
			-- Updating DataStoreKeyInfo:
			profile.KeyInfo = keyInfo
			-- Setting global updates:
			local globalUpdatesObject = profile.GlobalUpdates -- [GlobalUpdates]
			local oldGlobalUpdatesData = globalUpdatesObject._UpdatesLatest
			local newGlobalUpdatesData = loadedData.GlobalUpdates
			globalUpdatesObject._UpdatesLatest = newGlobalUpdatesData
			-- Setting MetaData:
			local sessionMetaData = profile.MetaData
			local latestMetaData = loadedData.MetaData
			
			for key in next, SETTINGS.MetaTagsUpdatedValues do
				sessionMetaData[key] = latestMetaData[key]
			end
			
			sessionMetaData.MetaTagsLatest = latestMetaData.MetaTags
			-- 5) Check if session still owns the profile: --
			local activeSession = loadedData.MetaData.ActiveSession
			local sessionLoadCount = loadedData.MetaData.SessionLoadCount
			local sessionOwnsProfile = false
			
			if type(activeSession) == "table" then
				sessionOwnsProfile = IsThisSession(activeSession) and sessionLoadCount == lastSessionLoadCount
			end
			
			local is_active = profile:IsActive()
			if sessionOwnsProfile == true then
				-- 6) Check for new global updates: --
				if is_active == true then -- Profile could've been released before the saving thread finished
					CheckForNewGlobalUpdates(profile, oldGlobalUpdatesData, newGlobalUpdatesData)
				end
			else
				-- Session no longer owns the profile:
				-- 7) Release profile if it hasn't been released yet: --
				if is_active == true then
					ReleaseProfileInternally(profile)
				end
				-- Cleanup reference in custom write queue:
				CustomWriteQueueMarkForCleanup(profile._ProfileStore._ProfileStoreLookup, profile._ProfileKey)
				-- Hop ready listeners:
				if profile._HopReady == false then
					profile._HopReady = true
					profile._HopReadyListeners:Fire()
				end
			end
			-- Signaling MetaTagsUpdated listeners after a possible external profile release was handled:
			profile.MetaTagsUpdated:Fire(profile.MetaData.MetaTagsLatest)
			-- Signaling KeyInfoUpdated listeners:
			profile.KeyInfoUpdated:Fire(keyInfo)
		elseif repeatSaveFlag == true then
			task.wait() -- Prevent infinite loop in case DataStore API does not yield
		end
	end
	ActiveProfileSaveJobs -= 1
end

----- Public functions -----

-- GlobalUpdates object:

local GlobalUpdates = {
	--[[
		_UpdatesLatest = {}, -- [table] {updateIndex, {{updateId, versionId, updateLocked, updateData}, ...}}
		_PendingUpdateLock = {updateId, ...} / nil, -- [table / nil]
		_PendingUpdateClear = {updateId, ...} / nil, -- [table / nil]
		
		_NewActiveUpdateListeners = [ScriptSignal] / nil, -- [table / nil]
		_NewLockedUpdateListeners = [ScriptSignal] / nil, -- [table / nil]
		
		_Profile = Profile / nil, -- [Profile / nil]
		
		_UpdateHandlerMode = true / nil, -- [bool / nil]
	--]]
}
GlobalUpdates.__index = GlobalUpdates

-- ALWAYS PUBLIC:
function GlobalUpdates:GetActiveUpdates() --> [table] {{updateId, updateData}, ...}
	local queryList = {}
	for _, globalUpdate in ipairs(self._UpdatesLatest[2]) do
		if globalUpdate[3] == false then
			local isPendingLock = false
			if self._PendingUpdateLock ~= nil then
				for _, updateId in ipairs(self._PendingUpdateLock) do
					if globalUpdate[1] == updateId then
						isPendingLock = true -- Exclude global updates pending to be locked
						break
					end
				end
			end
			if isPendingLock == false then
				table.insert(queryList, {globalUpdate[1], globalUpdate[4]})
			end
		end
	end
	return queryList
end

function GlobalUpdates:GetLockedUpdates() --> [table] {{updateId, updateData}, ...}
	local queryList = {}
	for _, globalUpdate in ipairs(self._UpdatesLatest[2]) do
		if globalUpdate[3] == true then
			local isPendingClear = false
			if self._PendingUpdateClear ~= nil then
				for _, updateId in ipairs(self._PendingUpdateClear) do
					if globalUpdate[1] == updateId then
						isPendingClear = true -- Exclude global updates pending to be cleared
						break
					end
				end
			end
			if isPendingClear == false then
				table.insert(queryList, {globalUpdate[1], globalUpdate[4]})
			end
		end
	end
	return queryList
end

-- ONLY WHEN FROM "Profile.GlobalUpdates":
function GlobalUpdates:ListenToNewActiveUpdate(listener) --> [ScriptConnection] listener(updateId, updateData)
	if type(listener) ~= "function" then
		error("Only a function can be set as listener in GlobalUpdates:ListenToNewActiveUpdate()")
	end
	
	local profile = self._Profile
	if self._UpdateHandlerMode == true then
		error("Can't listen to new global updates in ProfileStore:GlobalUpdateProfileAsync()")
	elseif self._NewActiveUpdateListeners == nil then
		error("Can't listen to new global updates in view mode")
	elseif profile:IsActive() == false then -- Check if profile is expired
		return { -- Do not connect listener if the profile is expired
			Disconnect = function() end,
		}
	end

	-- Connect listener:
	return self._NewActiveUpdateListeners:Connect(listener)
end

function GlobalUpdates:ListenToNewLockedUpdate(listener) --> [ScriptConnection] listener(updateId, updateData)
	if type(listener) ~= "function" then
		error("Only a function can be set as listener in GlobalUpdates:ListenToNewLockedUpdate()")
	end
	
	local profile = self._Profile
	if self._UpdateHandlerMode == true then
		error("Can't listen to new global updates in ProfileStore:GlobalUpdateProfileAsync()")
	elseif self._NewLockedUpdateListeners == nil then
		error("Can't listen to new global updates in view mode")
	elseif profile:IsActive() == false then -- Check if profile is expired
		return { -- Do not connect listener if the profile is expired
			Disconnect = function() end,
		}
	end

	-- Connect listener:
	return self._NewLockedUpdateListeners:Connect(listener)
end

function GlobalUpdates:LockActiveUpdate(updateId)
	if type(updateId) ~= "number" then
		error("Invalid updateId")
	end
	
	local profile = self._Profile
	if self._UpdateHandlerMode == true then
		error("Can't lock active global updates in ProfileStore:GlobalUpdateProfileAsync()")
	elseif self._PendingUpdateLock == nil then
		error("Can't lock active global updates in view mode")
	elseif profile:IsActive() == false then -- Check if profile is expired
		error("PROFILE EXPIRED - Can't lock active global updates")
	end
	
	-- Check if global update exists with given updateId
	local globalUpdateExists = nil
	for _, globalUpdate in ipairs(self._UpdatesLatest[2]) do
		if globalUpdate[1] == updateId then
			globalUpdateExists = globalUpdate
			break
		end
	end
	
	if globalUpdateExists ~= nil then
		local isPendingLock = false
		for _, lock_update_id in ipairs(self._PendingUpdateLock) do
			if updateId == lock_update_id then
				isPendingLock = true -- Exclude global updates pending to be locked
				break
			end
		end
		if isPendingLock == false and globalUpdateExists[3] == false then -- Avoid id duplicates in _PendingUpdateLock
			table.insert(self._PendingUpdateLock, updateId)
		end
	else
		error("Passed non-existant updateId")
	end
end

function GlobalUpdates:ClearLockedUpdate(updateId)
	if type(updateId) ~= "number" then
		error("Invalid updateId")
	end
	
	local profile = self._Profile
	if self._UpdateHandlerMode == true then
		error("Can't clear locked global updates in ProfileStore:GlobalUpdateProfileAsync()")
	elseif self._PendingUpdateClear == nil then
		error("Can't clear locked global updates in view mode")
	elseif profile:IsActive() == false then -- Check if profile is expired
		error("PROFILE EXPIRED - Can't clear locked global updates")
	end
	
	-- Check if global update exists with given updateId
	local globalUpdateExists = nil
	for _, globalUpdate in ipairs(self._UpdatesLatest[2]) do
		if globalUpdate[1] == updateId then
			globalUpdateExists = globalUpdate
			break
		end
	end

	if globalUpdateExists ~= nil then
		local isPendingClear = false
		for _, clearUpdateId in ipairs(self._PendingUpdateClear) do
			if updateId == clearUpdateId then
				isPendingClear = true -- Exclude global updates pending to be cleared
				break
			end
		end
		if isPendingClear == false and globalUpdateExists[3] == true then -- Avoid id duplicates in _PendingUpdateClear
			table.insert(self._PendingUpdateClear, updateId)
		end
	else
		error("Passed non-existant updateId")
	end
end

-- EXPOSED TO "updateHandler" DURING ProfileStore:GlobalUpdateProfileAsync() CALL
function GlobalUpdates:AddActiveUpdate(updateData)
	if type(updateData) ~= "table" then
		error("Invalid updateData")
	end
	
	if self._NewActiveUpdateListeners ~= nil then
		error("Can't add active global updates in loaded Profile; Use ProfileStore:GlobalUpdateProfileAsync()")
	elseif self._UpdateHandlerMode ~= true then
		error("Can't add active global updates in view mode; Use ProfileStore:GlobalUpdateProfileAsync()")
	end

	-- self._UpdatesLatest = {}, -- [table] {updateIndex, {{updateId, versionId, updateLocked, updateData}, ...}}
	local updatesLatest = self._UpdatesLatest
	local updateIndex = updatesLatest[1] + 1 -- Incrementing global update index
	updatesLatest[1] = updateIndex
	-- Add new active global update:
	table.insert(updatesLatest[2], {updateIndex, 1, false, updateData})
end

function GlobalUpdates:ChangeActiveUpdate(updateId, updateData)
	if type(updateId) ~= "number" then
		error("Invalid updateId")
	end
	
	if type(updateData) ~= "table" then
		error("Invalid updateData")
	end

	if self._NewActiveUpdateListeners ~= nil then
		error("Can't change active global updates in loaded Profile; Use ProfileStore:GlobalUpdateProfileAsync()")
	elseif self._UpdateHandlerMode ~= true then
		error("Can't change active global updates in view mode; Use ProfileStore:GlobalUpdateProfileAsync()")
	end
	
	-- self._UpdatesLatest = {}, -- [table] {updateIndex, {{updateId, versionId, updateLocked, updateData}, ...}}
	local updatesLatest = self._UpdatesLatest
	local getGlobalUpdate = nil
	for _, globalUpdate in ipairs(updatesLatest[2]) do
		if updateId == globalUpdate[1] then
			getGlobalUpdate = globalUpdate
			break
		end
	end

	if getGlobalUpdate ~= nil then
		if getGlobalUpdate[3] == true then
			error("Can't change locked global update")
		end
		getGlobalUpdate[2] += 1 -- Increment version id
		getGlobalUpdate[4] = updateData -- Set new global update data
	else
		error("Passed non-existant updateId")
	end
end

function GlobalUpdates:ClearActiveUpdate(updateId)
	if type(updateId) ~= "number" then
		error("Invalid updateId argument")
	end
	
	if self._NewActiveUpdateListeners ~= nil then
		error("Can't clear active global updates in loaded Profile; Use ProfileStore:GlobalUpdateProfileAsync()")
	elseif self._UpdateHandlerMode ~= true then
		error("Can't clear active global updates in view mode; Use ProfileStore:GlobalUpdateProfileAsync()")
	end
	
	-- self._UpdatesLatest = {}, -- [table] {updateIndex, {{updateId, versionId, updateLocked, updateData}, ...}}
	local updatesLatest = self._UpdatesLatest
	local getGlobalUpdateIndex = nil
	local getGlobalUpdate = nil
	for index, globalUpdate in ipairs(updatesLatest[2]) do
		if updateId == globalUpdate[1] then
			getGlobalUpdateIndex = index
			getGlobalUpdate = globalUpdate
			break
		end
	end

	if getGlobalUpdate ~= nil then
		if getGlobalUpdate[3] == true then
			error("Can't clear locked global update")
		end
		table.remove(updatesLatest[2], getGlobalUpdateIndex) -- Remove active global update
	else
		error("Passed non-existant updateId")
	end
end

-- Profile object:

local Profile = {
	--[[
		Data = {}, -- [table] -- Loaded once after ProfileStore:LoadProfileAsync() finishes
		MetaData = {}, -- [table] -- Updated with every auto-save
		GlobalUpdates = GlobalUpdates, -- [GlobalUpdates]
		
		_ProfileStore = ProfileStore, -- [ProfileStore]
		_ProfileKey = "", -- [string]
		
		_ReleaseListeners = [ScriptSignal] / nil, -- [table / nil]
		_HopReadyListeners = [ScriptSignal] / nil, -- [table / nil]
		_HopReady = false,
		
		_ViewMode = true / nil, -- [bool] or nil
		
		_LoadTimestamp = os.clock(),
		
		_IsUserMock = false, -- ProfileStore.Mock
		_mock_key_info = {},
	--]]
}
Profile.__index = Profile

function Profile:IsActive() --> [bool]
	local loadedProfiles = self._IsUserMock == true and self._ProfileStore._MockLoadedProfiles or self._ProfileStore._LoadedProfiles
	return loadedProfiles[self._ProfileKey] == self
end

function Profile:GetMetaTag(tagName) --> value
	local metaData = self.MetaData
	if metaData == nil then
		return nil
		-- error("This Profile hasn't been loaded before - MetaData not available")
	end
	return self.MetaData.MetaTags[tagName]
end

function Profile:SetMetaTag(tagName, value)
	if type(tagName) ~= "string" then
		error("tagName must be a string")
	elseif string.len(tagName) == 0 then
		error("Invalid tagName")
	end
	self.MetaData.MetaTags[tagName] = value
end

function Profile:Reconcile()
	ReconcileTable(self.Data, self._ProfileStore._ProfileTemplate)
end

function Profile:ListenToRelease(listener) --> [ScriptConnection] (placeId / nil, gameJobId / nil)
	if type(listener) ~= "function" then
		error("Only a function can be set as listener in Profile:ListenToRelease()")
	end
	if self._ViewMode == true then
		return {Disconnect = function() end}
	end
	if self:IsActive() == false then
		-- Call release listener immediately if profile is expired
		local placeId
		local gameJobId
		local activeSession = self.MetaData.ActiveSession
		if activeSession ~= nil then
			placeId = activeSession[1]
			gameJobId = activeSession[2]
		end
		listener(placeId, gameJobId)
		return {Disconnect = function() end}
	else
		return self._ReleaseListeners:Connect(listener)
	end
end

function Profile:Save()
	if self._ViewMode == true then
		error("Can't save Profile in view mode - Should you be calling :OverwriteAsync() instead?")
	end
	if self:IsActive() == false then
		warn("[ProfileService]: Attempted saving an inactive profile "
			.. self:Identify() .. "; Traceback:\n" .. debug.traceback())
		return
	end
	-- Reject save request if a save is already pending in the queue - this will prevent the user from
	--	unecessary API request spam which we could not meaningfully execute anyways!
	if IsCustomWriteQueueEmptyFor(self._ProfileStore._ProfileStoreLookup, self._ProfileKey) == true then
		-- We don't want auto save to trigger too soon after manual saving - this will reset the auto save timer:
		RemoveProfileFromAutoSave(self)
		AddProfileToAutoSave(self)
		-- Call save function in a new thread:
		task.spawn(SaveProfileAsync, self)
	end
end

function Profile:Release()
	if self._ViewMode == true then
		return
	end
	if self:IsActive() == true then
		task.spawn(SaveProfileAsync, self, true) -- Call save function in a new thread with releaseFromSession = true
	end
end

function Profile:ListenToHopReady(listener) --> [ScriptConnection] ()
	if type(listener) ~= "function" then
		error("Only a function can be set as listener in Profile:ListenToHopReady()")
	end
	if self._ViewMode == true then
		return {Disconnect = function() end}
	end
	if self._HopReady == true then
		task.spawn(listener)
		return {Disconnect = function() end}
	else
		return self._HopReadyListeners:Connect(listener)
	end
end

function Profile:AddUserId(userId) -- Associates userId with profile (GDPR compliance)
	if type(userId) ~= "number" or userId % 1 ~= 0 then
		warn("[ProfileService]: Invalid UserId argument for :AddUserId() ("
			.. tostring(userId) .. "); Traceback:\n" .. debug.traceback())
		return
	end

	if userId < 0 and self._IsUserMock ~= true and UseMockDataStore ~= true then
		return -- Avoid giving real Roblox APIs negative UserId's
	end

	if table.find(self.UserIds, userId) == nil then
		table.insert(self.UserIds, userId)
	end
end

function Profile:RemoveUserId(userId) -- Unassociates userId with profile (safe function)
	if type(userId) ~= "number" or userId % 1 ~= 0 then
		warn("[ProfileService]: Invalid UserId argument for :RemoveUserId() ("
			.. tostring(userId) .. "); Traceback:\n" .. debug.traceback())
		return
	end

	local index = table.find(self.UserIds, userId)

	if index ~= nil then
		table.remove(self.UserIds, index)
	end
end

function Profile:Identify() --> [string]
	return IdentifyProfile(
		self._ProfileStore._ProfileStoreName,
		self._ProfileStore._ProfileStoreScope,
		self._ProfileKey
	)
end

function Profile:ClearGlobalUpdates() -- Clears all global updates data from a profile payload
	if self._ViewMode ~= true then
		error(":ClearGlobalUpdates() can only be used in view mode")
	end

	local globalUpdatesObject = {
		_UpdatesLatest = {0, {}},
		_Profile = self,
	}
	setmetatable(globalUpdatesObject, GlobalUpdates)

	self.GlobalUpdates = globalUpdatesObject
end

function Profile:OverwriteAsync() -- Saves the profile to the DataStore and removes the session lock
	if self._ViewMode ~= true then
		error(":OverwriteAsync() can only be used in view mode")
	end

	SaveProfileAsync(self, nil, true)
end

-- ProfileVersionQuery object:

local ProfileVersionQuery = {
	--[[
		_ProfileStore = profileStore,
		_ProfileKey = profileKey,
		_SortDirection = sortDirection,
		_MinDate = minDate,
		_MaxDate = maxDate,

		_QueryPages = pages, -- [DataStoreVersionPages]
		_QueryIndex = index, -- [number]
		_QueryFailure = false,

		_IsQueryYielded = false,
		_QueryQueue = {},
	--]]
}
ProfileVersionQuery.__index = ProfileVersionQuery

function ProfileVersionQuery:_MoveQueue()
	while #self._QueryQueue > 0 do
		local queue_entry = table.remove(self._QueryQueue, 1)
		task.spawn(queue_entry)
		if self._IsQueryYielded == true then
			break
		end
	end
end

function ProfileVersionQuery:NextAsync(isStacking) --> [Profile] or nil

	if self._ProfileStore == nil then
		return nil
	end

	local profile
	local isFinished = false

	local function queryJob()

		if self._QueryFailure == true then
			isFinished = true
			return
		end

		-- First "next" call loads version pages:
		if self._QueryPages == nil then

			self._IsQueryYielded = true
			task.spawn(function()
				profile = self:NextAsync(true)
				isFinished = true
			end)

			local listSuccess, errorMessage = pcall(function()
				self._QueryPages = self._ProfileStore._GlobalDataStore:ListVersionsAsync(
					self._ProfileKey,
					self._SortDirection,
					self._MinDate,
					self._MaxDate
				)
				self._QueryIndex = 0
			end)

			if listSuccess == false or self._QueryPages == nil then
				warn("[ProfileService]: Version query fail - " .. tostring(errorMessage))
				self._QueryFailure = true
			end

			self._IsQueryYielded = false
			self:_MoveQueue()

			return
		end

		local current_page = self._QueryPages:GetCurrentPage()
		local nextItem = current_page[self._QueryIndex + 1]

		-- No more entries:
		if self._QueryPages.IsFinished == true and nextItem == nil then
			isFinished = true
			return
		end

		-- Load next page when this page is over:
		if nextItem == nil then

			self._IsQueryYielded = true
			task.spawn(function()
				profile = self:NextAsync(true)
				isFinished = true
			end)

			local success = pcall(function()
				self._QueryPages:AdvanceToNextPageAsync()
				self._QueryIndex = 0
			end)

			if success == false or #self._QueryPages:GetCurrentPage() == 0 then
				self._QueryFailure = true
			end

			self._IsQueryYielded = false
			self:_MoveQueue()

			return

		end

		-- Next page item:

		self._QueryIndex += 1
		profile = self._ProfileStore:ViewProfileAsync(self._ProfileKey, nextItem.Version)
		isFinished = true

	end

	if self._IsQueryYielded == false then
		queryJob()
	else
		if isStacking == true then
			table.insert(self._QueryQueue, 1, queryJob)
		else
			table.insert(self._QueryQueue, queryJob)
		end
	end

	while isFinished == false do
		task.wait()
	end

	return profile

end

-- ProfileStore object:

local ProfileStore = {
	--[[
		Mock = {},
	
		_ProfileStoreName = "", -- [string] -- DataStore name
		_ProfileStoreScope = nil, -- [string] or [nil] -- DataStore scope
		_ProfileStoreLookup = "", -- [string] -- _ProfileStoreName .. "\0" .. (_ProfileStoreScope or "")
		
		_ProfileTemplate = {}, -- [table]
		_GlobalDataStore = global_data_store, -- [GlobalDataStore] -- Object returned by DataStoreService:GetDataStore(_ProfileStoreName)
		
		_LoadedProfiles = {[profileKey] = Profile, ...},
		_ProfileLoadJobs = {[profileKey] = {loadId, loadedData}, ...},
		
		_MockLoadedProfiles = {[profileKey] = Profile, ...},
		_MockProfileLoadJobs = {[profileKey] = {loadId, loadedData}, ...},
	--]]
}
ProfileStore.__index = ProfileStore

function ProfileStore:LoadProfileAsync(profileKey, notReleasedHandler, useMock) --> [Profile / nil] notReleasedHandler(placeId, gameJobId)

	notReleasedHandler = notReleasedHandler or "ForceLoad"

	if self._ProfileTemplate == nil then
		error("Profile template not set - ProfileStore:LoadProfileAsync() locked for this ProfileStore")
	end

	if type(profileKey) ~= "string" then
		error("profileKey must be a string")
	elseif string.len(profileKey) == 0 then
		error("Invalid profileKey")
	end

	if type(notReleasedHandler) ~= "function" and notReleasedHandler ~= "ForceLoad" and notReleasedHandler ~= "Steal" then
		error("Invalid notReleasedHandler")
	end

	if ProfileService.ServiceLocked == true then
		return nil
	end

	WaitForPendingProfileStore(self)

	local isUserMock = useMock == UseMockTag

	-- Check if profile with profileKey isn't already loaded in this session:
	for _, profileStore in ipairs(ActiveProfileStores) do
		if profileStore._ProfileStoreLookup == self._ProfileStoreLookup then
			local loadedProfiles = isUserMock == true and profileStore._MockLoadedProfiles or profileStore._LoadedProfiles
			if loadedProfiles[profileKey] ~= nil then
				error("Profile " .. IdentifyProfile(self._ProfileStoreName, self._ProfileStoreScope, profileKey) .. " is already loaded in this session")
				-- Are you using Profile:Release() properly?
			end
		end
	end

	ActiveProfileLoadJobs += 1
	local forceLoad = notReleasedHandler == "ForceLoad"
	local forceLoadSteps = 0
	local requestForceLoad = forceLoad -- First step of ForceLoad
	local stealSession = false -- Second step of ForceLoad
	local aggressiveSteal = notReleasedHandler == "Steal" -- Developer invoked steal
	while ProfileService.ServiceLocked == false do
		-- Load profile:
		-- SPECIAL CASE - If LoadProfileAsync is called for the same key before another LoadProfileAsync finishes,
		-- yoink the DataStore return for the new call. The older call will return nil. This would prevent very rare
		-- game breaking errors where a player rejoins the server super fast.
		local profileLoadJobs = isUserMock == true and self._MockProfileLoadJobs or self._ProfileLoadJobs
		local loadedData, keyInfo
		local loadId = LoadIndex + 1
		LoadIndex = loadId
		local profileLoadJob = profileLoadJobs[profileKey] -- {loadId, {loadedData, keyInfo} or nil}
		
		if profileLoadJob ~= nil then
			profileLoadJob[1] = loadId -- Yoink load job
			while profileLoadJob[2] == nil do -- Wait for job to finish
				task.wait()
			end
			if profileLoadJob[1] == loadId then -- Load job hasn't been double-yoinked
				loadedData, keyInfo = table.unpack(profileLoadJob[2])
				profileLoadJobs[profileKey] = nil
			else
				ActiveProfileLoadJobs -= 1
				return nil
			end
		else
			profileLoadJob = {loadId, nil}
			profileLoadJobs[profileKey] = profileLoadJob
			profileLoadJob[2] = table.pack(StandardProfileUpdateAsyncDataStore(
				self,
				profileKey,
				{
					ExistingProfileHandle = function(latestData)
						if ProfileService.ServiceLocked == false then
							local activeSession = latestData.MetaData.ActiveSession
							local forceLoadSession = latestData.MetaData.ForceLoadSession
							-- IsThisSession(activeSession)
							if activeSession == nil then
								latestData.MetaData.ActiveSession = {PlaceId, JobId}
								latestData.MetaData.ForceLoadSession = nil
							elseif type(activeSession) == "table" then
								if IsThisSession(activeSession) == false then
									local last_update = latestData.MetaData.LastUpdate
									if last_update ~= nil then
										if os.time() - last_update > SETTINGS.AssumeDeadSessionLock then
											latestData.MetaData.ActiveSession = {PlaceId, JobId}
											latestData.MetaData.ForceLoadSession = nil
											return
										end
									end
									if stealSession == true or aggressiveSteal == true then
										local forceLoadUninterrupted = false
										if forceLoadSession ~= nil then
											forceLoadUninterrupted = IsThisSession(forceLoadSession)
										end
										if forceLoadUninterrupted == true or aggressiveSteal == true then
											latestData.MetaData.ActiveSession = {PlaceId, JobId}
											latestData.MetaData.ForceLoadSession = nil
										end
									elseif requestForceLoad == true then
										latestData.MetaData.ForceLoadSession = {PlaceId, JobId}
									end
								else
									latestData.MetaData.ForceLoadSession = nil
								end
							end
						end
					end,
					MissingProfileHandle = function(latestData)
						latestData.Data = DeepCopyTable(self._ProfileTemplate)
						latestData.MetaData = {
							ProfileCreateTime = os.time(),
							SessionLoadCount = 0,
							ActiveSession = {PlaceId, JobId},
							ForceLoadSession = nil,
							MetaTags = {},
						}
					end,
					EditProfile = function(latestData)
						if ProfileService.ServiceLocked == false then
							local activeSession = latestData.MetaData.ActiveSession
							if activeSession ~= nil and IsThisSession(activeSession) == true then
								latestData.MetaData.SessionLoadCount += 1
								latestData.MetaData.LastUpdate = os.time()
							end
						end
					end,
				},
				isUserMock
				))
			if profileLoadJob[1] == loadId then -- Load job hasn't been yoinked
				loadedData, keyInfo = table.unpack(profileLoadJob[2])
				profileLoadJobs[profileKey] = nil
			else
				ActiveProfileLoadJobs -= 1
				return nil -- Load job yoinked
			end
		end
		-- Handle load_data:
		if loadedData ~= nil and keyInfo ~= nil then
			local activeSession = loadedData.MetaData.ActiveSession
			if type(activeSession) == "table" then
				if IsThisSession(activeSession) == true then
					-- Special component in MetaTags:
					loadedData.MetaData.MetaTagsLatest = DeepCopyTable(loadedData.MetaData.MetaTags)
					-- Case #1: Profile is now taken by this session:
					-- Create Profile object:
					local globalUpdatesObject = {
						_UpdatesLatest = loadedData.GlobalUpdates,
						_PendingUpdateLock = {},
						_PendingUpdateClear = {},

						_NewActiveUpdateListeners = Madwork.NewScriptSignal(),
						_NewLockedUpdateListeners = Madwork.NewScriptSignal(),

						_Profile = nil,
					}
					setmetatable(globalUpdatesObject, GlobalUpdates)
					local profile = {
						Data = loadedData.Data,
						MetaData = loadedData.MetaData,
						MetaTagsUpdated = Madwork.NewScriptSignal(),

						RobloxMetaData = loadedData.RobloxMetaData or {},
						UserIds = loadedData.UserIds or {},
						KeyInfo = keyInfo,
						KeyInfoUpdated = Madwork.NewScriptSignal(),

						GlobalUpdates = globalUpdatesObject,

						_ProfileStore = self,
						_ProfileKey = profileKey,

						_ReleaseListeners = Madwork.NewScriptSignal(),
						_HopReadyListeners = Madwork.NewScriptSignal(),
						_HopReady = false,

						_LoadTimestamp = os.clock(),

						_IsUserMock = isUserMock,
					}
					setmetatable(profile, Profile)
					globalUpdatesObject._Profile = profile
					-- Referencing Profile object in ProfileStore:
					if next(self._LoadedProfiles) == nil and next(self._MockLoadedProfiles) == nil then -- ProfileStore object was inactive
						table.insert(ActiveProfileStores, self)
					end
					if isUserMock == true then
						self._MockLoadedProfiles[profileKey] = profile
					else
						self._LoadedProfiles[profileKey] = profile
					end
					-- Adding profile to AutoSaveList;
					AddProfileToAutoSave(profile)
					-- Special case - finished loading profile, but session is shutting down:
					if ProfileService.ServiceLocked == true then
						SaveProfileAsync(profile, true) -- Release profile and yield until the DataStore call is finished
						profile = nil -- nil will be returned by this call
					end
					-- Return Profile object:
					ActiveProfileLoadJobs -= 1
					return profile
				else
					-- Case #2: Profile is taken by some other session:
					if forceLoad == true then
						local forceLoadSession = loadedData.MetaData.ForceLoadSession
						local forceLoadUninterrupted = false
						if forceLoadSession ~= nil then
							forceLoadUninterrupted = IsThisSession(forceLoadSession)
						end
						if forceLoadUninterrupted == true then
							if requestForceLoad == false then
								forceLoadSteps += 1
								if forceLoadSteps == SETTINGS.ForceLoadMaxSteps then
									stealSession = true
								end
							end
							task.wait() -- Overload prevention
						else
							-- Another session tried to force load this profile:
							ActiveProfileLoadJobs -= 1
							return nil
						end
						requestForceLoad = false -- Only request a force load once
					elseif aggressiveSteal == true then
						task.wait() -- Overload prevention
					else
						local handlerResult = notReleasedHandler(activeSession[1], activeSession[2])
						if handlerResult == "Repeat" then
							task.wait() -- Overload prevention
						elseif handlerResult == "Cancel" then
							ActiveProfileLoadJobs -= 1
							return nil
						elseif handlerResult == "ForceLoad" then
							forceLoad = true
							requestForceLoad = true
							task.wait() -- Overload prevention
						elseif handlerResult == "Steal" then
							aggressiveSteal = true
							task.wait() -- Overload prevention
						else
							error(
								"[ProfileService]: Invalid return from notReleasedHandler (\"" .. tostring(handlerResult) .. "\")(" .. type(handlerResult) .. ");" ..
									"\n" .. IdentifyProfile(self._ProfileStoreName, self._ProfileStoreScope, profileKey) ..
									" Traceback:\n" .. debug.traceback()
							)
						end
					end
				end
			else
				ActiveProfileLoadJobs -= 1
				return nil -- In this scenario it is likely the ProfileService.ServiceLocked flag was raised
			end
		else
			task.wait() -- Overload prevention
		end
	end

	ActiveProfileLoadJobs -= 1
	return nil -- If loop breaks return nothing
end

function ProfileStore:GlobalUpdateProfileAsync(profileKey, updateHandler, useMock) --> [GlobalUpdates / nil] (updateHandler(GlobalUpdates))
	if type(profileKey) ~= "string" or string.len(profileKey) == 0 then
		error("Invalid profileKey")
	end
	if type(updateHandler) ~= "function" then
		error("Invalid updateHandler")
	end

	if ProfileService.ServiceLocked == true then
		return nil
	end

	WaitForPendingProfileStore(self)

	while ProfileService.ServiceLocked == false do
		-- Updating profile:
		local loadedData = StandardProfileUpdateAsyncDataStore(
			self,
			profileKey,
			{
				ExistingProfileHandle = nil,
				MissingProfileHandle = nil,
				EditProfile = function(latestData)
					-- Running updateHandler:
					local globalUpdatesObject = {
						_UpdatesLatest = latestData.GlobalUpdates,
						_UpdateHandlerMode = true,
					}
					setmetatable(globalUpdatesObject, GlobalUpdates)
					updateHandler(globalUpdatesObject)
				end,
			},
			useMock == UseMockTag
		)
		CustomWriteQueueMarkForCleanup(self._ProfileStoreLookup, profileKey)
		-- Handling loadedData:
		if loadedData ~= nil then
			-- Return GlobalUpdates object (Update successful):
			local globalUpdatesObject = {
				_UpdatesLatest = loadedData.GlobalUpdates,
			}
			setmetatable(globalUpdatesObject, GlobalUpdates)
			return globalUpdatesObject
		else
			task.wait() -- Overload prevention
		end
	end

	return nil -- Return nothing (Update unsuccessful)
end

function ProfileStore:ViewProfileAsync(profileKey, version, useMock) --> [Profile / nil]
	if type(profileKey) ~= "string" or string.len(profileKey) == 0 then
		error("Invalid profileKey")
	end

	if ProfileService.ServiceLocked == true then
		return nil
	end

	WaitForPendingProfileStore(self)

	if version ~= nil and (useMock == UseMockTag or UseMockDataStore == true) then
		return nil -- No version support in mock mode
	end

	while ProfileService.ServiceLocked == false do
		-- Load profile:
		local loadedData, keyInfo = StandardProfileUpdateAsyncDataStore(
			self,
			profileKey,
			{
				ExistingProfileHandle = nil,
				MissingProfileHandle = function(latestData)
					latestData.Data = DeepCopyTable(self._ProfileTemplate)
					latestData.MetaData = {
						ProfileCreateTime = os.time(),
						SessionLoadCount = 0,
						ActiveSession = nil,
						ForceLoadSession = nil,
						MetaTags = {},
					}
				end,
				EditProfile = nil,
			},
			useMock == UseMockTag,
			true, -- Use :GetAsync()
			version -- DataStore key version
		)
		CustomWriteQueueMarkForCleanup(self._ProfileStoreLookup, profileKey)
		-- Handle load_data:
		if loadedData ~= nil then

			if keyInfo == nil then
				return nil -- Load was successful, but the key was empty - return no profile object
			end

			-- Create Profile object:
			local globalUpdatesObject = {
				_UpdatesLatest = loadedData.GlobalUpdates, -- {0, {}}
				_Profile = nil,
			}
			setmetatable(globalUpdatesObject, GlobalUpdates)
			local profile = {
				Data = loadedData.Data,
				MetaData = loadedData.MetaData,
				MetaTagsUpdated = Madwork.NewScriptSignal(),

				RobloxMetaData = loadedData.RobloxMetaData or {},
				UserIds = loadedData.UserIds or {},
				KeyInfo = keyInfo,
				KeyInfoUpdated = Madwork.NewScriptSignal(),

				GlobalUpdates = globalUpdatesObject,

				_ProfileStore = self,
				_ProfileKey = profileKey,

				_ViewMode = true,

				_LoadTimestamp = os.clock(),
			}
			setmetatable(profile, Profile)
			globalUpdatesObject._Profile = profile
			-- Returning Profile object:
			return profile
		else
			task.wait() -- Overload prevention
		end
	end

	return nil -- If loop breaks return nothing
end

function ProfileStore:ProfileVersionQuery(profileKey, sortDirection, minDate, maxDate, useMock) --> [ProfileVersionQuery]
	if type(profileKey) ~= "string" or string.len(profileKey) == 0 then
		error("Invalid profileKey")
	end

	if ProfileService.ServiceLocked == true then
		return setmetatable({}, ProfileVersionQuery) -- Silently fail :Next() requests
	end

	WaitForPendingProfileStore(self)

	if useMock == UseMockTag or UseMockDataStore == true then
		error(":ProfileVersionQuery() is not supported in mock mode")
	end

	-- Type check:
	if sortDirection ~= nil and (typeof(sortDirection) ~= "EnumItem"
		or sortDirection.EnumType ~= Enum.SortDirection) then
		error("Invalid sortDirection (" .. tostring(sortDirection) .. ")")
	end

	if minDate ~= nil and typeof(minDate) ~= "DateTime" and typeof(minDate) ~= "number" then
		error("Invalid minDate (" .. tostring(minDate) .. ")")
	end

	if maxDate ~= nil and typeof(maxDate) ~= "DateTime" and typeof(maxDate) ~= "number" then
		error("Invalid maxDate (" .. tostring(maxDate) .. ")")
	end

	minDate = typeof(minDate) == "DateTime" and minDate.UnixTimestampMillis or minDate
	maxDate = typeof(maxDate) == "DateTime" and maxDate.UnixTimestampMillis or maxDate

	local profileVersionQuery = {
		_ProfileStore = self,
		_ProfileKey = profileKey,
		_SortDirection = sortDirection,
		_MinDate = minDate,
		_MaxDate = maxDate,

		_QueryPages = nil,
		_QueryIndex = 0,
		_QueryFailure = false,

		_IsQueryYielded = false,
		_QueryQueue = {},
	}
	setmetatable(profileVersionQuery, ProfileVersionQuery)

	return profileVersionQuery

end

function ProfileStore:WipeProfileAsync(profileKey, useMock) --> is_wipe_successful [bool]
	if type(profileKey) ~= "string" or string.len(profileKey) == 0 then
		error("Invalid profileKey")
	end

	if ProfileService.ServiceLocked == true then
		return false
	end

	WaitForPendingProfileStore(self)

	local wipeStatus = false

	if useMock == UseMockTag then -- Used when the profile is accessed through ProfileStore.Mock
		local mockDataStore = UserMockDataStore[self._ProfileStoreLookup]
		if mockDataStore ~= nil then
			mockDataStore[profileKey] = nil
		end
		wipeStatus = true
		task.wait() -- Simulate API call yield
	elseif UseMockDataStore == true then -- Used when API access is disabled
		local mockDataStore = MockDataStore[self._ProfileStoreLookup]
		if mockDataStore ~= nil then
			mockDataStore[profileKey] = nil
		end
		wipeStatus = true
		task.wait() -- Simulate API call yield
	else
		wipeStatus = pcall(function()
			self._GlobalDataStore:RemoveAsync(profileKey)
		end)
	end

	CustomWriteQueueMarkForCleanup(self._ProfileStoreLookup, profileKey)

	return wipeStatus
end

-- New ProfileStore:

function ProfileService.GetProfileStore(profileStoreIndex, profileTemplate) --> [ProfileStore]

	local profileStoreName
	local profileStoreScope = nil

	-- Parsing profileStoreIndex:
	if type(profileStoreIndex) == "string" then
		-- profileStoreIndex as string:
		profileStoreName = profileStoreIndex
	elseif type(profileStoreIndex) == "table" then
		-- profileStoreIndex as table:
		profileStoreName = profileStoreIndex.Name
		profileStoreScope = profileStoreIndex.Scope
	else
		error("Invalid or missing profileStoreIndex")
	end

	-- Type checking:
	if profileStoreName == nil or type(profileStoreName) ~= "string" then
		error("Missing or invalid \"Name\" parameter")
	elseif string.len(profileStoreName) == 0 then
		error("ProfileStore name cannot be an empty string")
	end

	if profileStoreScope ~= nil and (type(profileStoreScope) ~= "string" or string.len(profileStoreScope) == 0) then
		error("Invalid \"Scope\" parameter")
	end

	if type(profileTemplate) ~= "table" then
		error("Invalid profileTemplate")
	end

	local profileStore
	profileStore = {
		Mock = {
			LoadProfileAsync = function(_, profileKey, notReleasedHandler)
				return profileStore:LoadProfileAsync(profileKey, notReleasedHandler, UseMockTag)
			end,
			GlobalUpdateProfileAsync = function(_, profileKey, updateHandler)
				return profileStore:GlobalUpdateProfileAsync(profileKey, updateHandler, UseMockTag)
			end,
			ViewProfileAsync = function(_, profileKey, version)
				return profileStore:ViewProfileAsync(profileKey, version, UseMockTag)
			end,
			FindProfileVersionAsync = function(_, profileKey, sortDirection, minDate, maxDate)
				return profileStore:FindProfileVersionAsync(profileKey, sortDirection, minDate, maxDate, UseMockTag)
			end,
			WipeProfileAsync = function(_, profileKey)
				return profileStore:WipeProfileAsync(profileKey, UseMockTag)
			end
		},

		_ProfileStoreName = profileStoreName,
		_ProfileStoreScope = profileStoreScope,
		_ProfileStoreLookup = profileStoreName .. "\0" .. (profileStoreScope or ""),

		_ProfileTemplate = profileTemplate,
		_GlobalDataStore = nil,
		_LoadedProfiles = {},
		_ProfileLoadJobs = {},
		_MockLoadedProfiles = {},
		_MockProfileLoadJobs = {},
		_IsPending = false,
	}

	setmetatable(profileStore, ProfileStore)

	if IsLiveCheckActive == true then
		profileStore._IsPending = true
		task.spawn(function()
			WaitForLiveAccessCheck()
			if UseMockDataStore == false then
				profileStore._GlobalDataStore = DataStoreService:GetDataStore(profileStoreName, profileStoreScope)
			end
			profileStore._IsPending = false
		end)
	else
		if UseMockDataStore == false then
			profileStore._GlobalDataStore = DataStoreService:GetDataStore(profileStoreName, profileStoreScope)
		end
	end

	return profileStore
end

function ProfileService.IsLive() --> [bool] -- (CAN YIELD!!!)
	WaitForLiveAccessCheck()
	return UseMockDataStore == false
end

----- Initialize -----

if IsStudio == true then
	IsLiveCheckActive = true
	task.spawn(function()
		local status, message = pcall(function()
			-- This will error if current instance has no Studio API access:
			DataStoreService:GetDataStore("____PS"):SetAsync("____PS", os.time())
		end)
		local noInternetAccess = status == false and string.find(message, "ConnectFail", 1, true) ~= nil
		if noInternetAccess == true then
			warn("[ProfileService]: No internet access - check your network connection")
		end
		if status == false and
			(string.find(message, "403", 1, true) ~= nil or -- Cannot write to DataStore from studio if API access is not enabled
				string.find(message, "must publish", 1, true) ~= nil or -- Game must be published to access live keys
				noInternetAccess == true) then -- No internet access

			UseMockDataStore = true
			ProfileService._UseMockDataStore = true
			print("[ProfileService]: Roblox API services unavailable - data will not be saved")
		else
			print("[ProfileService]: Roblox API services available - data will be saved")
		end
		IsLiveCheckActive = false
	end)
end

----- Connections -----

-- Auto saving and issue queue managing:
RunService.Heartbeat:Connect(function()
	-- 1) Auto saving: --
	local autoSaveListLength = #AutoSaveList
	if autoSaveListLength > 0 then
		local autoSaveIndexSpeed = SETTINGS.AutoSaveProfiles / autoSaveListLength
		local os_clock = os.clock()
		while os_clock - LastAutoSave > autoSaveIndexSpeed do
			LastAutoSave += autoSaveIndexSpeed
			local profile = AutoSaveList[AutoSaveIndex]
			if os_clock - profile._LoadTimestamp < SETTINGS.AutoSaveProfiles then
				-- This profile is freshly loaded - auto-saving immediately after loading will cause a warning in the log:
				profile = nil
				for _ = 1, autoSaveListLength - 1 do
					-- Move auto save index to the right:
					AutoSaveIndex += 1
					if AutoSaveIndex > autoSaveListLength then
						AutoSaveIndex = 1
					end
					profile = AutoSaveList[AutoSaveIndex]
					if os_clock - profile._LoadTimestamp >= SETTINGS.AutoSaveProfiles then
						break
					else
						profile = nil
					end
				end
			end
			-- Move auto save index to the right:
			AutoSaveIndex += 1
			if AutoSaveIndex > autoSaveListLength then
				AutoSaveIndex = 1
			end
			-- Perform save call:
			if profile ~= nil then
				task.spawn(SaveProfileAsync, profile) -- Auto save profile in new thread
			end
		end
	end

	-- 2) Issue queue: --
	-- Critical state handling:
	if ProfileService.CriticalState == false then
		if #IssueQueue >= SETTINGS.IssueCountForCriticalState then
			ProfileService.CriticalState = true
			ProfileService.CriticalStateSignal:Fire(true)
			CriticalStateStart = os.clock()
			warn("[ProfileService]: Entered critical state")
		end
	else
		if #IssueQueue >= SETTINGS.IssueCountForCriticalState then
			CriticalStateStart = os.clock()
		elseif os.clock() - CriticalStateStart > SETTINGS.CriticalStateLast then
			ProfileService.CriticalState = false
			ProfileService.CriticalStateSignal:Fire(false)
			warn("[ProfileService]: Critical state ended")
		end
	end

	-- Issue queue:
	while true do
		local issueTime = IssueQueue[1]
		if issueTime == nil then
			break
		elseif os.clock() - issueTime > SETTINGS.IssueLast then
			table.remove(IssueQueue, 1)
		else
			break
		end
	end
end)

-- Release all loaded profiles when the server is shutting down:
task.spawn(function()
	WaitForLiveAccessCheck()
	Madwork.ConnectToOnClose(
		function()
			ProfileService.ServiceLocked = true
			-- 1) Release all active profiles: --
			-- Clone AutoSaveList to a new table because AutoSaveList changes when profiles are released:
			local onCloseSaveJobCount = 0
			local active_profiles = {}
			
			for index, profile in ipairs(AutoSaveList) do
				active_profiles[index] = profile
			end

			-- Release the profiles; Releasing profiles can trigger listeners that release other profiles, so check active state:
			for _, profile in ipairs(active_profiles) do
				if profile:IsActive() == true then
					onCloseSaveJobCount += 1
					task.spawn(function() -- Save profile on new thread
						SaveProfileAsync(profile, true)
						onCloseSaveJobCount -= 1
					end)
				end
			end

			-- 2) Yield until all active profile jobs are finished: --
			while onCloseSaveJobCount > 0 or ActiveProfileLoadJobs > 0 or ActiveProfileSaveJobs > 0 do
				task.wait()
			end

			return -- We're done!
		end,
		UseMockDataStore == false -- Always run this OnClose task if using Roblox API services
	)
end)

return ProfileService