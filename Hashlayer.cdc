// SPDX-License-Identifier: MIT
//
// HashLayer_v3_fixed.cdc
//
// HashLayer - manual deposits + ComputeShares + pay-to-use royalty system
// - No Forte/Actions dependency
// - Anyone can create tasks
// - Manual depositToTask(...) funds reward pool
// - Commit / Reveal -> accept -> mint ComputeShares
// - payToUse(...) allows users to buy time-limited access (cost = duration * payRatePerSecond)
// - Royalties accumulated per-share and claimable by ComputeShare owners
//
// NOTE: Test extensively in Flow Emulator / Testnet.

import FungibleToken from 0x9a0766d93b6608b7
import FlowToken from 0x7e60df042a9c0868
import Crypto from 0x631e88ae7f1d7c20

access(contract) contract HashLayer {

    // ---------------- Events ----------------
    pub event TaskCreated(taskId: UInt64, owner: Address)
    pub event CommitSubmitted(taskId: UInt64, worker: Address)
    pub event RevealSubmitted(taskId: UInt64, worker: Address, resultHash: String, revealCID: String?)
    pub event TaskAccepted(taskId: UInt64, winningHash: String, winners: [Address])

    pub event ComputeShareMinted(taskId: UInt64, shareId: UInt64, owner: Address)
    pub event ComputeShareDeposited(taskId: UInt64, shareId: UInt64, owner: Address, depositedToOwner: Bool)
    pub event FallbackShareStored(taskId: UInt64, shareId: UInt64, owner: Address)

    pub event UsagePaid(taskId: UInt64, payer: Address, duration: UFix64, amount: UFix64)
    pub event DepositToTask(taskId: UInt64, from: Address?, amount: UFix64)
    pub event RefundReturned(to: Address, amount: UFix64)
    pub event OwnerWithdraw(taskId: UInt64, owner: Address, amount: UFix64)
    pub event RoyaltiesClaimed(taskId: UInt64, claimer: Address, amount: UFix64)

    pub event NodeResultsFinalized(taskId: UInt64, winningHash: String, winners: [Address], payoutPerWinner: UFix64)

    // store row/share links (e.g., IPFS CID or URL) for offchain access
    pub event RowLinkStored(taskId: UInt64, shareId: UInt64, link: String)

    // ---------------- ComputeShare resource ----------------
    // NOTE: No row data stored on-chain; only rowHash kept and optionally a link is stored in mapping.
    access(contract) resource ComputeShare {
        pub let taskId: UInt64
        pub let ownerAddress: Address
        pub let rowHash: String

        init(taskId: UInt64, ownerAddress: Address, rowHash: String) {
            self.taskId = taskId
            self.ownerAddress = ownerAddress
            self.rowHash = rowHash
        }
    }

    // Receiver interface for account collections
    access(contract) resource interface ComputeShareReceiver {
        pub fun deposit(share: @ComputeShare, shareId: UInt64)
    }

    // Per-account collection resource
    access(contract) resource ComputeShareCollection: ComputeShareReceiver {
        access(contract) var owned: @{UInt64: ComputeShare}

        init() {
            self.owned <- {}
        }

        pub fun deposit(share: @ComputeShare, shareId: UInt64) {
            self.owned[shareId] <-! share
        }

        pub fun withdraw(shareId: UInt64): @ComputeShare {
            let s <- self.owned.remove(key: shareId)
                ?? panic("No share with that id in collection")
            return <- s
        }

        destroy() {
            destroy self.owned
        }
    }

    // Public helper to create a collection for external accounts
    pub fun createEmptyCollection(): @ComputeShareCollection {
        return <- create ComputeShareCollection()
    }

    // ---------------- Task resource ----------------
    access(contract) resource Task {
        pub let id: UInt64
        pub let owner: Address
        pub let dataCID: String
        pub let k: UInt64
        pub let reward: UFix64

        // Royalties: payRatePerSecond defines price (Flow) per second of access
        pub let payRatePerSecond: UFix64
        pub let defaultAccessWindow: UFix64

        // commit/reveal maps (worker -> hash)
        access(contract) var commits: {Address: String}
        access(contract) var reveals: {Address: String}
        access(contract) var revealCIDs: {Address: String}
        access(contract) var accepted: Bool

        // deadlines (seconds since epoch). Use 0.0 for no deadline.
        pub let commitDeadline: UFix64
        pub let revealDeadline: UFix64

        // Per-task vaults for rewards and royalties
        access(contract) var rewardVault: @FlowToken.Vault
        access(contract) var royaltyVault: @FlowToken.Vault

        init(
            id: UInt64,
            owner: Address,
            dataCID: String,
            k: UInt64,
            reward: UFix64,
            payRatePerSecond: UFix64,
            defaultAccessWindow: UFix64,
            commitDeadline: UFix64,
            revealDeadline: UFix64
        ) {
            self.id = id
            self.owner = owner
            self.dataCID = dataCID
            self.k = k
            self.reward = reward
            self.payRatePerSecond = payRatePerSecond
            self.defaultAccessWindow = defaultAccessWindow

            self.commits = {}
            self.reveals = {}
            self.revealCIDs = {}
            self.accepted = false
            self.commitDeadline = commitDeadline
            self.revealDeadline = revealDeadline

            self.rewardVault <- FlowToken.createEmptyVault()
            self.royaltyVault <- FlowToken.createEmptyVault()
        }

        destroy() {
            destroy self.rewardVault
            destroy self.royaltyVault
        }

        // Worker commits an answer hash
        pub fun commit(worker: Address, commitHash: String) {
            // enforce commitDeadline if set (>0)
            if self.commitDeadline > 0.0 {
                let now: UFix64 = getCurrentBlock().timestamp
                if now > self.commitDeadline {
                    panic("Commit deadline passed")
                }
            }
            self.commits[worker] = commitHash
        }

        // Worker reveals answer details (we only store a result hash for consensus)
        pub fun reveal(worker: Address, resultHash: String, revealCID: String?) {
            if self.revealDeadline > 0.0 {
                let now: UFix64 = getCurrentBlock().timestamp
                if now > self.revealDeadline {
                    panic("Reveal deadline passed")
                }
            }
            self.reveals[worker] = resultHash
            if revealCID != nil {
                self.revealCIDs[worker] = revealCID!
            }
        }

        pub fun getAllReveals(): {Address: String} {
            return self.reveals
        }

        pub fun markAccepted() {
            self.accepted = true
        }
    }

    // ---------------- Contract-level storage ----------------
    access(contract) var tasks: @{UInt64: Task}
    access(contract) var nextTaskId: UInt64

    // fallbackComputeShares[taskId] => @{ shareId: ComputeShare }
    access(contract) var fallbackComputeShares: @{UInt64: @{UInt64: ComputeShare}}
    access(contract) var nextShareId: UInt64

    // shareOwners[shareId] = ownerAddress
    access(contract) var shareOwners: {UInt64: Address}
    // taskShareIds[taskId] = [shareId,...]   (each share = one row assignment)
    access(contract) var taskShareIds: {UInt64: [UInt64]}
    // taskShareOwners[taskId] = [ownerAddress,...] (unique owners list)
    access(contract) var taskShareOwners: {UInt64: [Address]}

    // rowAssignments[taskId][worker] = [rowIndex,...]
    access(contract) var rowAssignments: {UInt64: {Address: [UInt64]}}
    // nodeResults[taskId][nodeAddr] = resultHash
    access(contract) var nodeResults: {UInt64: {Address: String}}

    // Accounting for royalties per-share:
    // royaltyPerShare[taskId] = cumulative royalties amount allocated per share (UFix64)
    access(contract) var royaltyPerShare: {UInt64: UFix64}
    // lastClaimedPerShare[taskId][owner] = snapshot of royaltyPerShare at last claim
    access(contract) var lastClaimedPerShare: {UInt64: {Address: UFix64}}

    // contract-level vault (temporary; most flows use task vaults)
    access(contract) var contractVault: @FlowToken.Vault

    // Track share counts per owner per task to allow O(1) claim
    access(contract) var ownerShareCount: {UInt64: {Address: UInt64}}

    // mapping to store row/share links (e.g., IPFS CID or URL)
    access(contract) var taskRowLinks: {UInt64: {UInt64: String}}

    // accessRegistry: who has access until when per task
    access(contract) var accessRegistry: {Address: {UInt64: UFix64}}

    init() {
        self.tasks <- {}
        self.fallbackComputeShares <- {}
        self.nextTaskId = 1
        self.nextShareId = 1

        self.shareOwners = {}
        self.taskShareIds = {}
        self.taskShareOwners = {}
        self.rowAssignments = {}
        self.nodeResults = {}

        self.royaltyPerShare = {}
        self.lastClaimedPerShare = {}

        self.contractVault <- FlowToken.createEmptyVault()

        self.ownerShareCount = {}
        self.taskRowLinks = {}
        self.accessRegistry = {}
    }

    destroy() {
        destroy self.tasks
        destroy self.fallbackComputeShares
        destroy self.contractVault
    }

    // ---------------- Task creation (anyone can create a task) ----------------
    //
    // Must be called from a transaction that passes the creator's AuthAccount
    //
    pub fun createTask(
        dataCID: String,
        k: UInt64,
        reward: UFix64,
        payRatePerSecond: UFix64,
        defaultAccessWindow: UFix64,
        commitDeadline: UFix64,
        revealDeadline: UFix64,
        creatorAcct: AuthAccount
    ): UInt64 {
        let id = self.nextTaskId
        self.nextTaskId = id + 1

        self.tasks[id] <- create Task(
            id: id,
            owner: creatorAcct.address,
            dataCID: dataCID,
            k: k,
            reward: reward,
            payRatePerSecond: payRatePerSecond,
            defaultAccessWindow: defaultAccessWindow,
            commitDeadline: commitDeadline,
            revealDeadline: revealDeadline
        )

        emit TaskCreated(taskId: id, owner: creatorAcct.address)
        return id
    }

    // ---------------- depositToTask ----------------
    // Anyone can deposit Flow tokens into a task's reward vault.
    // Caller expectation: passing a FlowToken.Vault -> entire vault will be deposited into rewardVault.
    pub fun depositToTask(taskId: UInt64, from: @FlowToken.Vault, depositor: AuthAccount?) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        // capture amount before moving resource
        let amount: UFix64 = from.balance
        tRef!.rewardVault.deposit(from: <- from)
        // depositor may be nil (e.g., called from contract-level). Use nil-able addr to emit.
        emit DepositToTask(taskId: taskId, from: depositor?.address, amount: amount)
    }

    // ---------------- commit / reveal (workers pass their AuthAccount) ----------------
    pub fun submitCommit(taskId: UInt64, commitHash: String, workerAcct: AuthAccount) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        tRef!.commit(workerAcct.address, commitHash)
        emit CommitSubmitted(taskId: taskId, worker: workerAcct.address)
    }

    pub fun submitReveal(taskId: UInt64, resultHash: String, revealCID: String?, workerAcct: AuthAccount) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        tRef!.reveal(workerAcct.address, resultHash, revealCID)
        emit RevealSubmitted(taskId: taskId, worker: workerAcct.address, resultHash: resultHash, revealCID: revealCID)
    }

    // ---------------- acceptMatrixAndDistributeShares (owner must sign) ----------------
    // Accepts rowHashes (off-chain computed) and requires the task owner to approve.
    // Mints ComputeShares for winners and records ownership. Task owner cannot be a claimant for royalties.
    pub fun acceptMatrixAndDistributeShares(
        taskId: UInt64,
        rowHashes: [String],
        ownerAcct: AuthAccount
    ) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        pre { ownerAcct.address == tRef!.owner: "Only task owner can accept" }
        let task = tRef!

        // Build frequency map of revealed results
        var freq: {String: [Address]} = {}
        let reveals = task.getAllReveals()
        for worker in reveals.keys {
            let resultHash = reveals[worker]!
            if freq[resultHash] == nil {
                freq[resultHash] = [worker]
            } else {
                var list = freq[resultHash]!
                list.append(worker)
                freq[resultHash] = list
            }
        }

        var bestHash: String? = nil
        var bestList: [Address] = []
        for h in freq.keys {
            let list = freq[h]!
            if bestHash == nil || list.length > bestList.length {
                bestHash = h
                bestList = list
            }
        }

        if bestHash == nil { panic("No reveals") }

        task.markAccepted()
        var winners: [Address] = bestList
        if winners.length == 0 { panic("No winners") }

        // Build eligible winners array (exclude task owner)
        var eligWinners: [Address] = []
        var windex = 0
        while windex < winners.length {
            if winners[windex] != task.owner {
                eligWinners.append(winners[windex])
            }
            windex = windex + 1
        }
        if eligWinners.length == 0 { panic("No eligible winners (task owner cannot be a winner)") }

        // prepare assignments: round-robin distribute rowHashes indices across elig winners
        var assignments: {Address: [UInt64]} = {}
        var i = 0
        while i < eligWinners.length {
            assignments[eligWinners[i]] = []
            i = i + 1
        }

        var rhIndex: UInt64 = 0
        let rhCount: UInt64 = UInt64(rowHashes.length)
        while rhIndex < rhCount {
            let winner = eligWinners[Int(rhIndex % UInt64(eligWinners.length))]
            var arr = assignments[winner]!
            arr.append(rhIndex)
            assignments[winner] = arr
            rhIndex = rhIndex + 1
        }

        // Ensure mapping containers exist for task
        if self.taskShareIds[taskId] == nil {
            self.taskShareIds[taskId] = []
        }
        if self.taskShareOwners[taskId] == nil {
            self.taskShareOwners[taskId] = []
        }
        if self.ownerShareCount[taskId] == nil {
            self.ownerShareCount[taskId] = {}
        }
        if self.taskRowLinks[taskId] == nil {
            self.taskRowLinks[taskId] = {}
        }

        // Ensure fallback map for this task exists (resource)
        if self.fallbackComputeShares[taskId] == nil {
            var inner: @{UInt64: ComputeShare} <- {}
            self.fallbackComputeShares[taskId] <-! inner
        }

        // Mint ComputeShares (no row data stored). Deposit if receiver exists, else fallback.
        var ei = 0
        while ei < eligWinners.length {
            let addr = eligWinners[ei]
            let sharesForWorker = assignments[addr]!
            var si = 0
            while si < sharesForWorker.length {
                let rowIndex = sharesForWorker[si]
                let rowHash = rowHashes[Int(rowIndex)]
                let shareId = self.nextShareId
                self.nextShareId = shareId + 1

                let share <- create ComputeShare(
                    taskId: taskId,
                    ownerAddress: addr,
                    rowHash: rowHash
                )

                // record owner and task mappings
                self.shareOwners[shareId] = addr
                var arrIds = self.taskShareIds[taskId]!
                arrIds.append(shareId)
                self.taskShareIds[taskId] = arrIds

                var ownersList = self.taskShareOwners[taskId]!
                var found = false
                var k = 0
                while k < ownersList.length {
                    if ownersList[k] == addr {
                        found = true
                        break
                    }
                    k = k + 1
                }
                if !found {
                    ownersList.append(addr)
                    self.taskShareOwners[taskId] = ownersList
                }

                // increment ownerShareCount O(1)
                var countMap = self.ownerShareCount[taskId]!
                let prevCount: UInt64 = countMap[addr] ?? 0
                countMap[addr] = prevCount + UInt64(1)
                self.ownerShareCount[taskId] = countMap

                // store row link placeholder - rowHash might be a content hash; you may supply/replace with actual CID link later
                if self.taskRowLinks[taskId] == nil {
                    self.taskRowLinks[taskId] = {}
                }
                self.taskRowLinks[taskId]![shareId] = rowHash
                emit RowLinkStored(taskId: taskId, shareId: shareId, link: rowHash)

                // Attempt to deposit into account collection capability; if absent, put in fallback
                let capability = getAccount(addr)
                    .getCapability(/public/HashLayerComputeShareReceiver)
                    .borrow<&{ComputeShareReceiver}>()

                if capability != nil {
                    capability!.deposit(share: <- share, shareId: shareId)
                    emit ComputeShareMinted(taskId: taskId, shareId: shareId, owner: addr)
                    emit ComputeShareDeposited(taskId: taskId, shareId: shareId, owner: addr, depositedToOwner: true)
                } else {
                    // fetch inner fallback dict, or create if somehow missing
                    var innerRef <- self.fallbackComputeShares.remove(key: taskId) ?? panic("fallback missing")
                    innerRef[shareId] <-! share
                    self.fallbackComputeShares[taskId] <-! innerRef
                    emit ComputeShareMinted(taskId: taskId, shareId: shareId, owner: addr)
                    emit ComputeShareDeposited(taskId: taskId, shareId: shareId, owner: addr, depositedToOwner: false)
                    emit FallbackShareStored(taskId: taskId, shareId: shareId, owner: addr)
                }
                si = si + 1
            }
            ei = ei + 1
        }

        // initialize royaltyPerShare snapshot for this task if absent
        if self.royaltyPerShare[taskId] == nil {
            self.royaltyPerShare[taskId] = 0.0
        }
        if self.lastClaimedPerShare[taskId] == nil {
            self.lastClaimedPerShare[taskId] = {}
        }

        emit TaskAccepted(taskId: taskId, winningHash: bestHash!, winners: winners)
    }

    // ---------------- payToUse ----------------
    // Payer pays Flow tokens to gain access for `duration` seconds.
    // Cost = duration * task.payRatePerSecond
    // Only allowed after the task has been accepted (compute shares exist).
    // Payment increases royaltyPerShare by amount / totalShares.
    //
    // Returns remainder vault (if caller passed more than cost).
    pub fun payToUse(
        taskId: UInt64,
        duration: UFix64,
        payerAcct: AuthAccount,
        from: @FlowToken.Vault
    ): @FlowToken.Vault {
        if duration <= 0.0 { destroy from; panic("duration must be > 0") }

        let tRef = &self.tasks[taskId] as &Task? ?? { destroy from; panic("Task not found") }()
        let task = tRef!

        if !task.accepted { destroy from; panic("Task must be accepted (compute shares minted) before payToUse") }

        // compute cost
        let cost: UFix64 = duration * task.payRatePerSecond

        if from.balance < cost {
            destroy from
            panic("Insufficient funds in provided vault for requested duration")
        }

        // withdraw exactly cost, deposit into task royalty vault
        let costVault <- from.withdraw(amount: cost)
        task.royaltyVault.deposit(from: <- costVault)

        // update per-share accounting
        let totalShares = self.taskShareIds[taskId] ?? []
        let totalSharesCount = totalShares.length
        if totalSharesCount == 0 {
            // safety: no shares means refund remainder and leave royalty as-is
            let refund <- from
            // emit usage paid even though no shares? choose to emit with amount 0 to indicate no-op (we'll emit refund)
            emit RefundReturned(to: payerAcct.address, amount: refund.balance)
            return <- refund
        }

        // increment cumulative royalty per share
        let perShareDelta: UFix64 = cost / UFix64(totalSharesCount)
        let prev: UFix64 = self.royaltyPerShare[taskId] ?? 0.0
        self.royaltyPerShare[taskId] = prev + perShareDelta

        // record access expiry for payer
        if self.accessRegistry[payerAcct.address] == nil {
            self.accessRegistry[payerAcct.address] = {}
        }
        let now = getCurrentBlock().timestamp
        let expiry = now + duration
        self.accessRegistry[payerAcct.address]![taskId] = expiry

        emit UsagePaid(taskId: taskId, payer: payerAcct.address, duration: duration, amount: cost)

        // return remainder to caller (may be empty)
        let remainder <- from
        let remBal: UFix64 = remainder.balance
        if remBal > 0.0 {
            emit RefundReturned(to: payerAcct.address, amount: remBal)
        }
        return <- remainder
    }

    // ---------------- claimRoyalties ----------------
    // A ComputeShare holder calls this (with their AuthAccount) to claim all available royalties
    // owed to them for a specific task. Task owner is explicitly forbidden from claiming.
    //
    pub fun claimRoyalties(taskId: UInt64, claimant: AuthAccount) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        let task = tRef!

        // task owner may NOT claim royalties
        if claimant.address == task.owner {
            panic("Task owner cannot claim royalties")
        }

        // ensure royaltyPerShare exists
        let cumulative = self.royaltyPerShare[taskId] ?? 0.0
        if cumulative == 0.0 {
            // nothing to claim
            return
        }

        // use ownerShareCount O(1)
        let countMap = self.ownerShareCount[taskId] ?? panic("No shares for task")
        let ownerCountUInt64 = countMap[claimant.address] ?? 0
        if ownerCountUInt64 == 0 {
            panic("Claimant owns no compute shares for this task")
        }
        let ownerShareCount: UFix64 = UFix64(ownerCountUInt64)

        // prepare lastClaim snapshot for this owner
        if self.lastClaimedPerShare[taskId] == nil {
            self.lastClaimedPerShare[taskId] = {}
        }
        let lastClaimSnapshot: UFix64 = self.lastClaimedPerShare[taskId]![claimant.address] ?? 0.0

        if cumulative <= lastClaimSnapshot {
            // nothing new to claim
            return
        }

        let deltaPerShare: UFix64 = cumulative - lastClaimSnapshot
        // amount owed = deltaPerShare * ownerShareCount
        let owed: UFix64 = deltaPerShare * ownerShareCount

        // update last claimed snapshot
        self.lastClaimedPerShare[taskId]![claimant.address] = cumulative

        // withdraw owed from task royalty vault and send to claimant
        let claimerReceiver = getAccount(claimant.address)
            .getCapability(/public/flowTokenReceiver)
            .borrow<&{FungibleToken.Receiver}>()

        if claimerReceiver == nil {
            // If claimant has no flowTokenReceiver, revert (you could accept leaving owed in vault instead)
            panic("Claimant has not linked a Flow token receiver at /public/flowTokenReceiver")
        }

        let pay <- task.royaltyVault.withdraw(amount: owed)
        claimerReceiver!.deposit(from: <- pay)

        emit RoyaltiesClaimed(taskId: taskId, claimer: claimant.address, amount: owed)
    }

    // ---------------- finalizeNodeResults ----------------
    // Pays winners pro-rata from the task.rewardVault. If any winner cannot accept, leave funds in vault.
    pub fun finalizeNodeResults(taskId: UInt64) {
        let resultsMap = self.nodeResults[taskId] ?? panic("No node results for task")
        var freq: {String: [Address]} = {}
        for nodeAddr in resultsMap.keys {
            let h = resultsMap[nodeAddr]!
            if freq[h] == nil {
                freq[h] = [nodeAddr]
            } else {
                var list = freq[h]!
                list.append(nodeAddr)
                freq[h] = list
            }
        }

        var bestHash: String? = nil
        var bestList: [Address] = []
        for h in freq.keys {
            let list = freq[h]!
            if bestHash == nil || list.length > bestList.length {
                bestHash = h
                bestList = list
            }
        }

        if bestHash == nil { panic("No results submitted") }

        let winners: [Address] = bestList
        let winnersCount = winners.length
        if winnersCount == 0 { panic("No winners") }

        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        let task = tRef!

        let escrowedBalance: UFix64 = task.rewardVault.balance
        if escrowedBalance <= 0.0 { panic("No escrowed funds for task") }

        // equal split to winners (you may change to proportional if you track shares per-row)
        let perWinner: UFix64 = escrowedBalance / UFix64(winnersCount)

        var idx = 0
        while idx < winnersCount {
            let w = winners[idx]
            // Skip task owner if somehow included
            if w == task.owner {
                idx = idx + 1
                continue
            }
            let wReceiver = getAccount(w)
                .getCapability(/public/flowTokenReceiver)
                .borrow<&{FungibleToken.Receiver}>()
            if wReceiver != nil {
                let pay <- task.rewardVault.withdraw(amount: perWinner)
                wReceiver!.deposit(from: <- pay)
            } else {
                // if winner has no receiver, skip and leave funds in vault
            }
            idx = idx + 1
        }

        // leftover (dust or skipped winners) remains in task.rewardVault
        emit NodeResultsFinalized(taskId: taskId, winningHash: bestHash!, winners: winners, payoutPerWinner: perWinner)
    }

    // ---------------- Claim fallback compute shares helper ----------------
    // Allows an account to claim fallback (un-deposited) compute shares into their registered collection.
    // Caller must be the claimant (AuthAccount passed).
    pub fun claimFallbackShares(taskId: UInt64, claimant: AuthAccount) {
        // ensure claimant has a collection capability
        let receiverCap = claimant.getCapability(/public/HashLayerComputeShareReceiver)
            .borrow<&{ComputeShareReceiver}>()
        if receiverCap == nil {
            panic("Claimant has not registered a HashLayerComputeShareReceiver at /public/HashLayerComputeShareReceiver")
        }

        // attempt to remove fallback mapping for task
        if self.fallbackComputeShares[taskId] == nil {
            return
        }
        var map <- self.fallbackComputeShares.remove(key: taskId)!
        //  iterate and deposit any shares whose owner equals claimant.address
        for shareId in map.keys {
            let shareOwner = self.shareOwners[shareId] ?? panic("share owner missing")
            if shareOwner == claimant.address {
                let outShare <- map.remove(key: shareId) as @ComputeShare
                receiverCap!.deposit(share: <- outShare, shareId: shareId)
                emit ComputeShareDeposited(taskId: taskId, shareId: shareId, owner: claimant.address, depositedToOwner: true)
            }
        }
        // put back the possibly reduced map
        if map.keys.length > 0 {
            self.fallbackComputeShares[taskId] <-! map
        } else {
            destroy map
        }
    }

    // ---------------- Owner withdraw reward / cancel task ----------------
    // Allows the task owner to withdraw leftover reward (when desired). Leaves royaltyVault untouched.
    pub fun ownerWithdrawReward(taskId: UInt64, ownerAcct: AuthAccount, amount: UFix64) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        pre { ownerAcct.address == tRef!.owner: "Only task owner can withdraw rewards" }
        let task = tRef!

        if amount <= 0.0 { panic("amount must be > 0") }
        if task.rewardVault.balance < amount { panic("Insufficient reward balance") }

        let receiver = getAccount(ownerAcct.address)
            .getCapability(/public/flowTokenReceiver)
            .borrow<&{FungibleToken.Receiver}>() ?? panic("Owner has not linked /public/flowTokenReceiver")

        let pay <- task.rewardVault.withdraw(amount: amount)
        receiver.deposit(from: <- pay)
        emit OwnerWithdraw(taskId: taskId, owner: ownerAcct.address, amount: amount)
    }

    // ---------------- Row Link setter (owner or contract) ----------------
    // Allows storing a link (e.g., IPFS CID or URL) for a specific share (or row).
    // You can call this after minting to replace the placeholder rowHash with an actual URL
    pub fun setRowLink(taskId: UInt64, shareId: UInt64, link: String, setter: AuthAccount) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        // only task owner or contract admin (for now setter==owner) can set link; adapt as needed
        pre { setter.address == tRef!.owner: "Only task owner can set row links" }

        if self.taskRowLinks[taskId] == nil {
            self.taskRowLinks[taskId] = {}
        }
        self.taskRowLinks[taskId]![shareId] = link
        emit RowLinkStored(taskId: taskId, shareId: shareId, link: link)
    }

    // ---------------- Views / helpers ----------------

    pub fun getShareIdsForTask(taskId: UInt64): [UInt64] {
        return self.taskShareIds[taskId] ?? []
    }

    pub fun getShareOwnersForTask(taskId: UInt64): [Address] {
        return self.taskShareOwners[taskId] ?? []
    }

    pub fun getRowLink(taskId: UInt64, shareId: UInt64): String? {
        return self.taskRowLinks[taskId]?[shareId]
    }

    // Returns a moved copy of fallback shares for on-chain inspection; caller must destroy
    pub fun takeFallbackSharesForTask(taskId: UInt64): @{UInt64: ComputeShare}? {
        if self.fallbackComputeShares[taskId] == nil {
            return nil
        }
        let map <- self.fallbackComputeShares.remove(key: taskId)!
        return <- map
    }

    // View whether a user currently has access to a task
    pub fun hasAccess(user: Address, taskId: UInt64): Bool {
        let m = self.accessRegistry[user] ?? {}
        let expiry = m[taskId] ?? 0.0
        let now = getCurrentBlock().timestamp
        return expiry > now
    }
}