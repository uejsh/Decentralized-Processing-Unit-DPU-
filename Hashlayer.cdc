

// HashLayerWithActions.cdc
//
// Compatible with Flow Actions framework
// Purpose: Allow royalty payments for share usage via atomic Flow Actions

import FungibleToken from 0x9a0766d93b6608b7
import FlowToken from 0x7e60df042a9c0868
import Actions from 0xForteActions
import Crypto from 0x631e88ae7f1d7c20

access(all) contract HashLayerWithActions {

    // ---------------- Events ----------------
    pub event TaskCreated(id: UInt64, owner: Address)
    pub event CommitSubmitted(taskId: UInt64, worker: Address)
    pub event RevealSubmitted(taskId: UInt64, worker: Address, ipfsHash: String)
    pub event ShareAssigned(taskId: UInt64, to: Address, shareCount: UInt64)
    pub event RoyaltyPaid(taskId: UInt64, payer: Address, amount: UFix64)
    pub event RoyaltiesDistributed(taskId: UInt64, total: UFix64)

    // ---------------- Task + Ownership Data ----------------
    pub struct Task {
        pub let id: UInt64
        pub let owner: Address
        pub var committedHash: String
        pub var revealedHash: String?
        pub var totalShares: UInt64
        pub var shareOwners: {Address: UInt64}  // owner → shares owned

        init(id: UInt64, owner: Address, committedHash: String) {
            self.id = id
            self.owner = owner
            self.committedHash = committedHash
            self.revealedHash = nil
            self.totalShares = 0
            self.shareOwners = {}
        }
    }

    access(self) var tasks: {UInt64: Task}
    access(self) var nextTaskID: UInt64

    // ---------------- Vaults ----------------
    access(self) var royaltyVault: @FlowToken.Vault

    // ---------------- Initialization ----------------
    init() {
        self.tasks = {}
        self.nextTaskID = 0
        self.royaltyVault <- FlowToken.createEmptyVault()
    }

    destroy() {
        destroy self.royaltyVault
    }

    // ---------------- Core Task Logic ----------------

    pub fun createTask(committedHash: String): UInt64 {
        let id = self.nextTaskID
        self.nextTaskID = self.nextTaskID + 1

        let newTask = Task(id: id, owner: self.account.address, committedHash: committedHash)
        self.tasks[id] = newTask

        emit TaskCreated(id: id, owner: self.account.address)
        return id
    }

    pub fun revealTask(id: UInt64, revealedHash: String) {
        let task = self.tasks[id] ?? panic("Task not found")
        task.revealedHash = revealedHash
        emit RevealSubmitted(taskId: id, worker: self.account.address, ipfsHash: revealedHash)
    }

    // ---------------- Share Assignment ----------------
    // Assign shares to addresses (e.g., after successful computation)
    pub fun assignShares(taskId: UInt64, owners: [Address], shares: [UInt64]) {
        let task = self.tasks[taskId] ?? panic("Task not found")
        pre {
            owners.length == shares.length: "Owners and shares length mismatch"
        }

        var total: UInt64 = 0
        var i = 0
        while i < owners.length {
            let addr = owners[i]
            let count = shares[i]
            task.shareOwners[addr] = (task.shareOwners[addr] ?? 0) + count
            total = total + count
            emit ShareAssigned(taskId: taskId, to: addr, shareCount: count)
            i = i + 1
        }

        task.totalShares = total
    }

    // ---------------- Royalty Sink (for Flow Actions) ----------------

    pub resource RoyaltySink: Actions.Sink {
        access(contract) let taskId: UInt64

        init(taskId: UInt64) {
            self.taskId = taskId
        }

        // Deposit FlowToken vaults as royalties
        pub fun depositTokens(from: @FlowToken.Vault): Bool {
            HashLayerWithActions.depositRoyalty(taskId: self.taskId, from: <-from)
            return true
        }
    }

    // Contract-owned function to create sink
    pub fun createRoyaltySink(taskId: UInt64): @RoyaltySink {
        return <- create RoyaltySink(taskId: taskId)
    }

    // ---------------- Royalty Logic ----------------

    // Receive FlowTokens into royaltyVault
    access(contract) fun depositRoyalty(taskId: UInt64, from: @FlowToken.Vault) {
        self.royaltyVault.deposit(from: <-from)
        emit RoyaltyPaid(taskId: taskId, payer: self.account.address, amount: self.royaltyVault.balance)
    }

    // Distribute accumulated royalties among owners
    pub fun distributeRoyalties(taskId: UInt64) {
        let task = self.tasks[taskId] ?? panic("Task not found")
        let totalShares = task.totalShares
        if totalShares == 0 {
            panic("No owners to distribute to")
        }

        let total = self.royaltyVault.balance
        var distributed: UFix64 = 0.0

        for owner in task.shareOwners.keys {
            let share = task.shareOwners[owner]!
            let portion = (UFix64(share) / UFix64(totalShares)) * total

            let vault <- self.royaltyVault.withdraw(amount: portion)
            let receiver = getAccount(owner)
                .getCapability(/public/flowTokenReceiver)
                .borrow<&{FungibleToken.Receiver}>()
                ?? panic("Missing receiver for owner")

            receiver.deposit(from: <-vault)
            distributed = distributed + portion
        }

        emit RoyaltiesDistributed(taskId: taskId, total: distributed)
    }
}