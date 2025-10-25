// SPDX-License-Identifier: MIT
//
// HashLayer.cdc
//
// Decentralized compute consensus + royalty layer
// - Per-task vaults for royalties & escrow
// - Commit / Reveal -> consensus -> mint OutputShares -> pay-per-use with split + royalties + escrow
// - Flow Actions sink per-task to deposit royalties atomically
//
// NOTE: Test extensively on Flow emulator / Testnet before deploying to Mainnet.

import FungibleToken from 0x9a0766d93b6608b7
import FlowToken from 0x7e60df042a9c0868
import Actions from 0xForteActions
import Crypto from 0x631e88ae7f1d7c20

access(all) contract HashLayer {

    // ---------------- Events ----------------
    pub event TaskCreated(taskId: UInt64, owner: Address)
    pub event CommitSubmitted(taskId: UInt64, worker: Address)
    pub event RevealSubmitted(taskId: UInt64, worker: Address, resultHash: String)
    pub event TaskAccepted(taskId: UInt64, winningHash: String, winners: [Address])

    pub event OutputShareMinted(taskId: UInt64, shareId: UInt64, owner: Address)
    pub event OutputShareDeposited(taskId: UInt64, shareId: UInt64, owner: Address, depositedToOwner: Bool)

    pub event UsagePaid(taskId: UInt64, payer: Address, usageFee: UFix64, splitToShares: UFix64, escrowed: UFix64)
    pub event RewardSplitToShares(taskId: UInt64, totalSharesSplit: UFix64, perShareOwner: UFix64)
    pub event NodeResultsFinalized(taskId: UInt64, winningHash: String, winners: [Address], payoutPerWinner: UFix64)

    // ---------------- OutputShare resource ----------------
    // NOTE: No row data stored on-chain; only rowHash kept.
    access(all) resource OutputShare {
        access(all) let taskId: UInt64
        access(all) let ownerAddress: Address
        access(all) let rowHash: String

        init(taskId: UInt64, ownerAddress: Address, rowHash: String) {
            self.taskId = taskId
            self.ownerAddress = ownerAddress
            self.rowHash = rowHash
        }
    }

    // Receiver interface for account collections
    access(all) resource interface OutputShareReceiver {
        access(all) fun deposit(share: @OutputShare, shareId: UInt64)
    }

    // Per-account collection resource
    access(all) resource OutputShareCollection: OutputShareReceiver {
        access(all) var owned: @{UInt64: OutputShare}

        init() {
            self.owned <- {}
        }

        access(all) fun deposit(share: @OutputShare, shareId: UInt64) {
            self.owned[shareId] <-! share
        }

        access(all) fun withdraw(shareId: UInt64): @OutputShare {
            let s <- self.owned.remove(key: shareId)
                ?? panic("No share with that id in collection")
            return <- s
        }

        destroy() {
            destroy self.owned
        }
    }

    pub fun createEmptyCollection(): @OutputShareCollection {
        return <- create OutputShareCollection()
    }

    // ---------------- Task resource ----------------
    access(all) resource Task {
        access(all) let id: UInt64
        access(all) let owner: Address
        access(all) let dataCID: String
        access(all) let k: UInt64
        access(all) let reward: UFix64

        // Royalties: percent of usageFee that goes directly to task owner (0.0 - 1.0)
        access(all) let royaltyPct: UFix64

        // commit/reveal maps (worker -> hash)
        access(self) var commits: {Address: String}
        access(self) var reveals: {Address: String}
        access(self) var revealCIDs: {Address: String}
        access(self) var accepted: Bool

        // deadlines (seconds since epoch). Use 0.0 for no deadline.
        access(all) let commitDeadline: UFix64
        access(all) let revealDeadline: UFix64

        // Per-task vault for royalties / escrow / payouts
        access(self) var taskVault: @FlowToken.Vault

        init(
            id: UInt64,
            owner: Address,
            dataCID: String,
            k: UInt64,
            reward: UFix64,
            royaltyPct: UFix64,
            commitDeadline: UFix64,
            revealDeadline: UFix64
        ) {
            self.id = id
            self.owner = owner
            self.dataCID = dataCID
            self.k = k
            self.reward = reward
            self.royaltyPct = royaltyPct
            self.commits = {}
            self.reveals = {}
            self.revealCIDs = {}
            self.accepted = false
            self.commitDeadline = commitDeadline
            self.revealDeadline = revealDeadline
            self.taskVault <- FlowToken.createEmptyVault()
        }

        destroy() {
            destroy self.taskVault
        }

        // Worker commits an answer hash
        access(all) fun commit(worker: Address, commitHash: String) {
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
        access(all) fun reveal(worker: Address, resultHash: String, revealCID: String?) {
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

        access(all) fun getAllReveals(): {Address: String} {
            return self.reveals
        }

        access(all) fun markAccepted() {
            self.accepted = true
        }
    }

    // ---------------- Contract-level storage ----------------
    access(self) var tasks: @{UInt64: Task}
    access(self) var nextTaskId: UInt64

    // fallbackOutputShares[taskId] => @{ shareId: OutputShare }
    access(self) var fallbackOutputShares: @{UInt64: @{UInt64: OutputShare}}
    access(self) var nextShareId: UInt64

    // shareOwners[shareId] = ownerAddress
    access(self) var shareOwners: {UInt64: Address}
    // taskShareIds[taskId] = [shareId,...]
    access(self) var taskShareIds: {UInt64: [UInt64]}
    // taskShareOwners[taskId] = [ownerAddress,...]
    access(self) var taskShareOwners: {UInt64: [Address]}

    // rowAssignments[taskId][worker] = [rowIndex,...]
    access(self) var rowAssignments: {UInt64: {Address: [UInt64]}}
    // nodeResults[taskId][nodeAddr] = resultHash
    access(self) var nodeResults: {UInt64: {Address: String}}

    // contract-level vault (temporary, but most flows deposit directly to taskVault)
    access(self) var contractVault: @FlowToken.Vault

    init() {
        self.tasks <- {}
        self.fallbackOutputShares <- {}
        self.nextTaskId = 1
        self.nextShareId = 1

        self.shareOwners = {}
        self.taskShareIds = {}
        self.taskShareOwners = {}

        self.rowAssignments = {}
        self.nodeResults = {}

        self.contractVault <- FlowToken.createEmptyVault()
    }

    destroy() {
        destroy self.tasks
        destroy self.fallbackOutputShares
        destroy self.contractVault
    }

    // ---------------- Task lifecycle helpers ----------------

    pub fun createTask(
        dataCID: String,
        k: UInt64,
        reward: UFix64,
        royaltyPct: UFix64,
        commitDeadline: UFix64,
        revealDeadline: UFix64
    ): UInt64 {
        // validate royaltyPct range
        if royaltyPct < 0.0 || royaltyPct > 1.0 {
            panic("royaltyPct must be between 0.0 and 1.0")
        }

        let id = self.nextTaskId
        self.nextTaskId = id + 1

        self.tasks[id] <- create Task(
            id: id,
            owner: self.account.address,
            dataCID: dataCID,
            k: k,
            reward: reward,
            royaltyPct: royaltyPct,
            commitDeadline: commitDeadline,
            revealDeadline: revealDeadline
        )

        emit TaskCreated(taskId: id, owner: self.account.address)
        return id
    }

    // external-facing commit/reveal wrappers (workers call these)
    pub fun submitCommit(taskId: UInt64, commitHash: String) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        tRef!.commit(self.account.address, commitHash)
        emit CommitSubmitted(taskId: taskId, worker: self.account.address)
    }

    pub fun submitReveal(taskId: UInt64, resultHash: String, revealCID: String?) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        tRef!.reveal(self.account.address, resultHash, revealCID)
        emit RevealSubmitted(taskId: taskId, worker: self.account.address, resultHash: resultHash)
    }

    // ---------------- Flow Actions Royalty Sink ----------------
    // Provides a per-task Actions.Sink that deposits directly into that task's vault.

    pub resource RoyaltySink: Actions.Sink {
        access(contract) let taskId: UInt64

        init(taskId: UInt64) {
            self.taskId = taskId
        }

        // deposit FlowToken vault into the task's vault
        pub fun depositTokens(from: @FlowToken.Vault): Bool {
            HashLayer.depositToTaskVault(taskId: self.taskId, from: <- from)
            return true
        }
    }

    pub fun createRoyaltySink(taskId: UInt64): @RoyaltySink {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        // only task owner can create a sink for their task
        pre { self.account.address == tRef!.owner: "Only task owner can create royalty sink" }
        return <- create RoyaltySink(taskId: taskId)
    }

    // internal function used by RoyaltySink to move funds into task vault
    access(contract) fun depositToTaskVault(taskId: UInt64, from: @FlowToken.Vault) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        tRef!.taskVault.deposit(from: <- from)
        // emit event with deposited amount for off-chain indexing
        emit UsagePaid(taskId: taskId, payer: self.account.address, usageFee: 0.0, splitToShares: 0.0, escrowed: 0.0)
        // Note: specific UsagePaid event for Action deposits can be added if desired
    }

    // ---------------- acceptMatrixAndDistributeShares (owner must sign) ----------------
    // Accepts rowHashes (off-chain computed) and requires the task owner to approve.
    access(all) fun acceptMatrixAndDistributeShares(
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
        let winners: [Address] = bestList
        if winners.length == 0 { panic("No winners") }

        // prepare assignments: map winner -> [rowIndex,...] (UInt64 indices)
        var assignments: {Address: [UInt64]} = {}
        var w = 0
        while w < winners.length {
            assignments[winners[w]] = []
            w = w + 1
        }

        var rhIndex: UInt64 = 0
        while rhIndex < UInt64(rowHashes.length) {
            let winner = winners[Int(rhIndex % UInt64(winners.length))]
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

        // Ensure fallback map for this task exists (resource)
        if self.fallbackOutputShares[taskId] == nil {
            var inner: @{UInt64: OutputShare} <- {}
            self.fallbackOutputShares[taskId] <-! inner
        }

        // Mint OutputShares (no row data stored). Deposit if receiver exists, else fallback.
        var wi = 0
        while wi < winners.length {
            let addr = winners[wi]
            let sharesForWorker = assignments[addr]!
            var si = 0
            while si < sharesForWorker.length {
                let rowIndex = sharesForWorker[si]
                let rowHash = rowHashes[Int(rowIndex)]
                let shareId = self.nextShareId
                self.nextShareId = shareId + 1

                let share <- create OutputShare(
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

                // Attempt to deposit into account collection capability; if absent, put in fallback
                let capability = getAccount(addr)
                    .getCapability(/public/HashLayerOutputShareReceiver)
                    .borrow<&{OutputShareReceiver}>()

                if capability != nil {
                    capability!.deposit(share: <- share, shareId: shareId)
                    emit OutputShareMinted(taskId: taskId, shareId: shareId, owner: addr)
                    emit OutputShareDeposited(taskId: taskId, shareId: shareId, owner: addr, depositedToOwner: true)
                } else {
                    // fetch inner fallback dict, or create if somehow missing
                    var innerRef <- self.fallbackOutputShares.remove(key: taskId) ?? panic("fallback missing")
                    innerRef[shareId] <-! share
                    self.fallbackOutputShares[taskId] <-! innerRef
                    emit OutputShareMinted(taskId: taskId, shareId: shareId, owner: addr)
                    emit OutputShareDeposited(taskId: taskId, shareId: shareId, owner: addr, depositedToOwner: false)
                }
                si = si + 1
            }
            wi = wi + 1
        }

        emit TaskAccepted(taskId: taskId, winningHash: bestHash!, winners: winners)
    }

    // ---------------- payToUseFunction ----------------
    // - splitToSharesPct: split among current share owners immediately (if possible)
    // - royaltyPct (per-task) goes to task owner immediately if possible
    // - remainder stays inside taskVault as escrow for future finalization
    access(all) fun payToUseFunction(taskId: UInt64, payer: AuthAccount, usageFee: UFix64) {
        if usageFee <= 0.0 { panic("usageFee must be > 0") }

        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        let task = tRef!

        // Withdraw from payer and deposit directly into taskVault (avoids contract-level mixing)
        let payerVault = payer.borrow<&FlowToken.Vault>(from: /storage/flowTokenVault)
            ?? panic("Cannot borrow payer vault")
        let payment <- payerVault.withdraw(amount: usageFee)
        task.taskVault.deposit(from: <- payment)

        // constants and safe checks
        let splitToSharesPct: UFix64 = 0.05
        let royaltyPct: UFix64 = task.royaltyPct
        pre { splitToSharesPct + royaltyPct <= 1.0: "Split percentages exceed 100%" }

        let splitToSharesAmount: UFix64 = usageFee * splitToSharesPct
        let royaltyAmount: UFix64 = usageFee * royaltyPct
        // remainder intentionally left in taskVault as escrow
        let escrowAmount: UFix64 = usageFee - splitToSharesAmount - royaltyAmount

        // 1) Distribute splitToSharesAmount among current share owners (equal per-owner)
        let owners = self.taskShareOwners[taskId] ?? []
        let ownersCount = owners.length
        if ownersCount > 0 {
            // compute per-owner with truncation rules of UFix64 division
            let perOwner: UFix64 = splitToSharesAmount / UFix64(ownersCount)
            var distributedTotal: UFix64 = 0.0
            var idx = 0
            while idx < ownersCount {
                let o = owners[idx]
                let maybeReceiver = getAccount(o)
                    .getCapability(/public/flowTokenReceiver)
                    .borrow<&{FungibleToken.Receiver}>()

                if maybeReceiver != nil {
                    let pay <- task.taskVault.withdraw(amount: perOwner)
                    maybeReceiver!.deposit(from: <- pay)
                } else {
                    // Owner has no receiver linked; keep their portion in the task vault (escrow)
                    // No withdraw; portion remains in task.taskVault
                }
                distributedTotal = distributedTotal + perOwner
                idx = idx + 1
            }

            // leftover due to division (dust) remains in task.taskVault
            emit RewardSplitToShares(taskId: taskId, totalSharesSplit: splitToSharesAmount, perShareOwner: perOwner)
        } else {
            // No owners yet: leave split amount as escrow in taskVault
            // already deposited above; nothing to do
        }

        // 2) Pay royalty to task owner immediately if possible, otherwise leave in taskVault
        if royaltyAmount > 0.0 {
            let ownerAddr = task.owner
            let ownerReceiver = getAccount(ownerAddr)
                .getCapability(/public/flowTokenReceiver)
                .borrow<&{FungibleToken.Receiver}>()
            if ownerReceiver != nil {
                let payRoyal <- task.taskVault.withdraw(amount: royaltyAmount)
                ownerReceiver!.deposit(from: <- payRoyal)
            } else {
                // leave royalty in taskVault as escrow
            }
        }

        // 3) remainder already left in taskVault as escrow
        emit UsagePaid(taskId: taskId, payer: payer.address, usageFee: usageFee, splitToShares: splitToSharesAmount, escrowed: escrowAmount)
    }

    // ---------------- finalizeNodeResults ----------------
    // Pays winners pro-rata from the taskVault. If any winner cannot accept, leave funds in vault.
    access(all) fun finalizeNodeResults(taskId: UInt64) {
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

        let escrowedBalance: UFix64 = task.taskVault.balance
        if escrowedBalance <= 0.0 { panic("No escrowed funds for task") }

        // equal split to winners (you may change to proportional if you track shares per-row)
        let perWinner: UFix64 = escrowedBalance / UFix64(winnersCount)

        var idx = 0
        while idx < winnersCount {
            let w = winners[idx]
            let wReceiver = getAccount(w)
                .getCapability(/public/flowTokenReceiver)
                .borrow<&{FungibleToken.Receiver}>()
            if wReceiver != nil {
                let pay <- task.taskVault.withdraw(amount: perWinner)
                wReceiver!.deposit(from: <- pay)
            } else {
                // if winner has no receiver, skip and leave funds in vault
            }
            idx = idx + 1
        }

        // leftover (dust or skipped winners) remains in task.taskVault
        emit NodeResultsFinalized(taskId: taskId, winningHash: bestHash!, winners: winners, payoutPerWinner: perWinner)
    }

    // ---------------- Claim fallback shares helper ----------------
    // Allows an account to claim fallback (un-deposited) output shares into their registered collection.
    // Caller must be the claimant (AuthAccount passed).
    pub fun claimFallbackShares(taskId: UInt64, claimant: AuthAccount) {
        // ensure claimant has a collection capability
        let receiverCap = claimant.getCapability(/public/HashLayerOutputShareReceiver)
            .borrow<&{OutputShareReceiver}>()
        if receiverCap == nil {
            panic("Claimant has not registered a HashLayerOutputShareReceiver at /public/HashLayerOutputShareReceiver")
        }

        // attempt to remove fallback mapping for task
        if self.fallbackOutputShares[taskId] == nil {
            return
        }
        var map <- self.fallbackOutputShares.remove(key: taskId)!
        // iterate and deposit any shares whose owner equals claimant.address
        for shareId in map.keys {
            let shareOwner = self.shareOwners[shareId] ?? panic("share owner missing")
            if shareOwner == claimant.address {
                let outShare <- map.remove(key: shareId) as @OutputShare
                receiverCap!.deposit(share: <- outShare, shareId: shareId)
            }
        }
        // put back the possibly reduced map
        if map.keys.length > 0 {
            self.fallbackOutputShares[taskId] <-! map
        } else {
            destroy map
        }
    }

    // ---------------- Views / helpers ----------------

    pub fun getShareIdsForTask(taskId: UInt64): [UInt64] {
        return self.taskShareIds[taskId] ?? []
    }

    pub fun getShareOwnersForTask(taskId: UInt64): [Address] {
        return self.taskShareOwners[taskId] ?? []
    }

    // Returns a moved copy of fallback shares for on-chain inspection; caller must destroy
    pub fun takeFallbackSharesForTask(taskId: UInt64): @{UInt64: OutputShare}? {
        if self.fallbackOutputShares[taskId] == nil {
            return nil
        }
        let map <- self.fallbackOutputShares.remove(key: taskId)!
        return <- map
    }
}