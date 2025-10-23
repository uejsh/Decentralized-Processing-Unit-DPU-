
import FungibleToken from 0xFUNGIBLETOKEN
import FlowToken from 0xFLOWTOKEN
import Crypto

pub contract HashLayer {

    // ---------------- Events ----------------
    pub event TaskCreated(id: UInt64, owner: Address)
    pub event CommitSubmitted(taskId: UInt64, worker: Address)
    pub event RevealSubmitted(taskId: UInt64, worker: Address, ipfsCID: String?)
    pub event OutputShareMinted(taskId: UInt64, shareId: UInt64, owner: Address)
    pub event OutputPaid(taskId: UInt64, shareId: UInt64, payer: Address, amount: UFix64)

    // ---------------- Row-level hasher ----------------
    pub fun hashRow(row: [UInt64]): String {
        var rowBytes: [UInt8] = []

        for value in row {
            rowBytes.append(UInt8((value >> 56) & 0xff))
            rowBytes.append(UInt8((value >> 48) & 0xff))
            rowBytes.append(UInt8((value >> 40) & 0xff))
            rowBytes.append(UInt8((value >> 32) & 0xff))
            rowBytes.append(UInt8((value >> 24) & 0xff))
            rowBytes.append(UInt8((value >> 16) & 0xff))
            rowBytes.append(UInt8((value >> 8) & 0xff))
            rowBytes.append(UInt8(value & 0xff))
        }

        let hashBytes: [UInt8] = Crypto.sha3_256(rowBytes)
        let hexChars = "0123456789abcdef".utf8
        var hexString = ""
        for byte in hashBytes {
            let high = Int(byte) / 16
            let low = Int(byte) % 16
            hexString = hexString.concat(String.fromCharCode(hexChars[high]))
            hexString = hexString.concat(String.fromCharCode(hexChars[low]))
        }
        return hexString
    }

    // ---------------- OutputShare resource ----------------
    pub resource OutputShare {
        pub let taskId: UInt64
        pub let ownerAddress: Address
        pub let rowHash: String

        init(taskId: UInt64, ownerAddress: Address, rowHash: String) {
            self.taskId = taskId
            self.ownerAddress = ownerAddress
            self.rowHash = rowHash
        }
    }

    // ---------------- Task resource ----------------
    pub resource Task {
        pub let id: UInt64
        pub let owner: Address
        pub let dataCID: String
        pub let k: UInt64
        pub let reward: UFix64

        access(self) var commits: {Address: String}
        access(self) var reveals: {Address: String}
        access(self) var revealCIDs: {Address: String}
        access(self) var accepted: Bool

        init(id: UInt64, owner: Address, dataCID: String, k: UInt64, reward: UFix64) {
            self.id = id
            self.owner = owner
            self.dataCID = dataCID
            self.k = k
            self.reward = reward
            self.commits = {}
            self.reveals = {}
            self.revealCIDs = {}
            self.accepted = false
        }

        pub fun commit(worker: Address, commitHash: String) {
            pre { !self.accepted: "Task already accepted" }
            self.commits[worker] = commitHash
            emit CommitSubmitted(taskId: self.id, worker: worker)
        }

        pub fun reveal(worker: Address, resultHash: String, ipfsCID: String?) {
            pre { !self.accepted: "Task already accepted" }
            if self.commits[worker] == nil {
                panic("Worker did not commit")
            }
            self.reveals[worker] = resultHash
            if ipfsCID != nil {
                self.revealCIDs[worker] = ipfsCID!
            }
            emit RevealSubmitted(taskId: self.id, worker: worker, ipfsCID: ipfsCID)
        }

        pub fun getAllReveals(): {Address: String} {
            return self.reveals
        }

        pub fun markAccepted() {
            self.accepted = true
        }
    }

    // ---------------- Contract-level storage ----------------
    access(self) var tasks: @{UInt64: Task}
    access(self) var nextTaskId: UInt64
    access(self) var outputShares: @{UInt64: @{UInt64: OutputShare}}
    access(self) var nextShareId: UInt64

    init() {
        self.tasks <- {}
        self.outputShares <- {}
        self.nextTaskId = 1
        self.nextShareId = 1
    }

    // ---------------- Task creation ----------------
    pub fun createTask(owner: Address, dataCID: String, k: UInt64, reward: UFix64): UInt64 {
        let id = self.nextTaskId
        self.nextTaskId = id + 1

        let t <- create Task(id: id, owner: owner, dataCID: dataCID, k: k, reward: reward)
        self.tasks[id] <-! t
        self.outputShares[id] <- {}
        emit TaskCreated(id: id, owner: owner)
        return id
    }

    // ---------------- Commit / Reveal ----------------
    pub fun commitFor(taskId: UInt64, worker: Address, commitHash: String) {
        let t = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        t!.commit(worker: worker, commitHash: commitHash)
    }

    pub fun revealFor(taskId: UInt64, worker: Address, resultHash: String, ipfsCID: String?) {
        let t = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        t!.reveal(worker: worker, resultHash: resultHash, ipfsCID: ipfsCID)
    }

    // ---------------- Accept majority matrix & distribute row OutputShares ----------------
    pub fun acceptMatrixAndDistributeShares(
        taskId: UInt64,
        matrix: [[UInt64]],
        caller: AuthAccount
    ) {
        let t = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        pre { caller.address == t!.owner: "Only task owner can accept" }

        // 1️⃣ Determine majority matrix
        var freq: {String: [Address]} = {}
        for (worker, resultHash) in t!.getAllReveals() {
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

        if bestHash == nil {
            panic("No reveals")
        }

        t!.markAccepted()

        let winners: [Address] = bestList

        // 2️⃣ Compute row hashes
        var rowHashes: [String] = []
        for row in matrix {
            let rowHash = self.hashRow(row: row)
            rowHashes.append(rowHash)
        }

        // 3️⃣ Randomly assign row hashes to winners (simple round-robin for hackathon)
        var assignments: {Address: [String]} = {}
        for addr in winners {
            assignments[addr] = []
        }

        var winnerIndex = 0
        let winnerCount = winners.length

        for rowHash in rowHashes {
            let winner = winners[winnerIndex % winnerCount]
            assignments[winner]!.append(rowHash)
            winnerIndex = winnerIndex + 1
        }

        // 4️⃣ Mint OutputShares
        for addr in winners {
            let sharesForWorker = assignments[addr]!
            for rowHash in sharesForWorker {
                let shareId = self.nextShareId
                self.nextShareId = shareId + 1
                let share <- create OutputShare(
                    taskId: taskId,
                    ownerAddress: addr,
                    rowHash: rowHash
                )
                if self.outputShares[taskId] == nil {
                    self.outputShares[taskId] <- {}
                }
                self.outputShares[taskId]![shareId] <-! share
                emit OutputShareMinted(taskId: taskId, shareId: shareId, owner: addr)
            }
        }
    }

    // ---------------- Pay to access output ----------------
    pub fun payForOutput(
        taskId: UInt64,
        shareId: UInt64,
        payer: AuthAccount,
        usageFee: UFix64
    ) {
        let share = self.outputShares[taskId]![shareId]!
        let payerVault = payer.borrow<&FlowToken.Vault>(from: /storage/flowTokenVault)
            ?? panic("Cannot borrow payer vault")

        let ownerVault = getAccount(share.ownerAddress)
            .getCapability(/public/flowTokenReceiver)
            .borrow<&FlowToken.Vault{FungibleToken.Receiver}>()
            ?? panic("Cannot borrow owner vault")

        let payment <- payerVault.withdraw(amount: usageFee)
        ownerVault.deposit(from: <- payment)
        emit OutputPaid(taskId: taskId, shareId: shareId, payer: payer.address, amount: usageFee)
    }
}
