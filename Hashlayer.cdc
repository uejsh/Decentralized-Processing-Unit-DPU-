// HashLayer.cdc
import FungibleToken from 0x9a0766d93b6608b7
import FlowToken from 0x7e60df042a9c0868
import Crypto from 0x631e88ae7f1d7c20

pub contract HashLayer {

    // ---------------- Events ----------------
    pub event TaskCreated(id: UInt64, owner: Address)
    pub event CommitSubmitted(taskId: UInt64, worker: Address)
    pub event RevealSubmitted(taskId: UInt64, worker: Address, ipfsCID: String?)
    pub event OutputShareMinted(taskId: UInt64, shareId: UInt64, owner: Address)
    pub event OutputShareDeposited(taskId: UInt64, shareId: UInt64, owner: Address, depositedToOwner: Bool)
    pub event OutputPaid(taskId: UInt64, shareId: UInt64, payer: Address, recipient: Address, amount: UFix64)

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

    // Receiver interface for account collections
    pub resource interface OutputShareReceiver {
        pub fun deposit(share: @OutputShare, shareId: UInt64)
    }

    // Per-account collection resource (users store this in their account storage)
    pub resource OutputShareCollection: OutputShareReceiver {
        pub var owned: @{UInt64: OutputShare}

        init() {
            self.owned <- {}
        }

        pub fun deposit(share: @OutputShare, shareId: UInt64) {
            self.owned[shareId] <-! share
        }

        pub fun withdraw(shareId: UInt64): @OutputShare {
            let s <- self.owned.remove(key: shareId)
                    ?? panic("No share with that id in collection")
            return <- s
        }

        destroy() {
            destroy self.owned
        }
    }

    // Public helper for users to create an empty collection and link it:
    //
    // tx (in account):
    //   let col <- HashLayer.createEmptyCollection()
    //   acct.save(<- col, to: /storage/HashLayerOutputShares)
    //   acct.link<&HashLayer.OutputShareCollection{HashLayer.OutputShareReceiver}>(
    //       /public/HashLayerOutputShareReceiver,
    //       target: /storage/HashLayerOutputShares
    //   )
    pub fun createEmptyCollection(): @OutputShareCollection {
        return <- create OutputShareCollection()
    }

    // ---------------- Task resource ----------------
    // commit = hex(sha3_256(resultHashUTF8 || nonceUTF8))
    pub resource Task {
        pub let id: UInt64
        pub let owner: Address
        pub let dataCID: String
        pub let k: UInt64
        pub let reward: UFix64

        // worker -> commit hash hex string
        access(self) var commits: {Address: String}
        // worker -> revealed resultHash (string)
        access(self) var reveals: {Address: String}
        // worker -> optional ipfs cid for reveal
        access(self) var revealCIDs: {Address: String}
        access(self) var accepted: Bool
        // optional deadlines (block timestamp) - set to 0 to ignore
        pub let commitDeadline: UFix64
        pub let revealDeadline: UFix64

        init(
            id: UInt64,
            owner: Address,
            dataCID: String,
            k: UInt64,
            reward: UFix64,
            commitDeadline: UFix64,
            revealDeadline: UFix64
        ) {
            self.id = id
            self.owner = owner
            self.dataCID = dataCID
            self.k = k
            self.reward = reward
            self.commits = {}
            self.reveals = {}
            self.revealCIDs = {}
            self.accepted = false
            self.commitDeadline = commitDeadline
            self.revealDeadline = revealDeadline
        }

        pub fun commit(worker: Address, commitHash: String) {
            pre { !self.accepted: "Task already accepted" }
            // optional deadline check
            if self.commitDeadline > 0.0 {
                let now = getCurrentBlock().timestamp
                pre { now <= self.commitDeadline: "Commit window closed" }
            }
            // store commit (hex string of sha3)
            self.commits[worker] = commitHash
            emit CommitSubmitted(taskId: self.id, worker: worker)
        }

        // Reveal must include nonce used in commit preimage
        pub fun reveal(worker: Address, resultHash: String, nonce: String, ipfsCID: String?) {
            pre { !self.accepted: "Task already accepted" }
            if self.commits[worker] == nil {
                panic("Worker did not commit")
            }
            // optional reveal deadline check
            if self.revealDeadline > 0.0 {
                let now = getCurrentBlock().timestamp
                pre { now <= self.revealDeadline: "Reveal window closed" }
            }

            // recompute commit preimage: bytes(resultHash) ++ bytes(nonce)
            var b: [UInt8] = []
            let rbytes: [UInt8] = resultHash.utf8
            var i = 0
            while i < rbytes.length {
                b.append(rbytes[i])
                i = i + 1
            }
            let nbytes: [UInt8] = nonce.utf8
            i = 0
            while i < nbytes.length {
                b.append(nbytes[i])
                i = i + 1
            }

            let recomputedBytes: [UInt8] = Crypto.sha3_256(b)
            let recomputedHex: String = String.encodeHex(recomputedBytes)

            let committed = self.commits[worker]!
            if recomputedHex != committed {
                panic("Reveal does not match commit preimage")
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
    // map taskId -> @Task
    access(self) var tasks: @{UInt64: Task}
    access(self) var nextTaskId: UInt64

    // map taskId -> map(shareId -> @OutputShare) - fallback storage when recipient doesn't have a collection
    access(self) var fallbackOutputShares: @{UInt64: @{UInt64: OutputShare}}
    access(self) var nextShareId: UInt64

    init() {
        self.tasks <- {}
        self.fallbackOutputShares <- {}
        self.nextTaskId = 1
        self.nextShareId = 1
    }

    // ---------------- Row hasher ----------------
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
        return String.encodeHex(hashBytes)
    }

    // ---------------- Task creation ----------------
    // commitDeadline & revealDeadline are block timestamps (UFix64). Use 0.0 to ignore deadlines.
    pub fun createTask(
        owner: Address,
        dataCID: String,
        k: UInt64,
        reward: UFix64,
        commitDeadline: UFix64,
        revealDeadline: UFix64
    ): UInt64 {
        let id = self.nextTaskId
        self.nextTaskId = id + 1

        let t <- create Task(
            id: id,
            owner: owner,
            dataCID: dataCID,
            k: k,
            reward: reward,
            commitDeadline: commitDeadline,
            revealDeadline: revealDeadline
        )
        self.tasks[id] <-! t
        self.fallbackOutputShares[id] <- {}
        emit TaskCreated(id: id, owner: owner)
        return id
    }

    // ---------------- Commit / Reveal wrappers ----------------
    pub fun commitFor(taskId: UInt64, worker: Address, commitHash: String) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        tRef.commit(worker: worker, commitHash: commitHash)
    }

    // Reveal wrapper requires nonce param
    pub fun revealFor(taskId: UInt64, worker: Address, resultHash: String, nonce: String, ipfsCID: String?) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        tRef.reveal(worker: worker, resultHash: resultHash, nonce: nonce, ipfsCID: ipfsCID)
    }

    // ---------------- Accept majority matrix & distribute row OutputShares ----------------
    // Note: transactions that call this should pass the signer's address as `callerAddress`
    pub fun acceptMatrixAndDistributeShares(
        taskId: UInt64,
        matrix: [[UInt64]],
        callerAddress: Address
    ) {
        let tRef = &self.tasks[taskId] as &Task? ?? panic("Task not found")
        pre { callerAddress == tRef.owner: "Only task owner can accept" }

        // Build frequency map: resultHash -> [Address]
        var freq: {String: [Address]} = {}
        for (worker, resultHash) in tRef.getAllReveals() {
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

        tRef.markAccepted()
        let winners: [Address] = bestList

        // Compute row hashes
        var rowHashes: [String] = []
        for row in matrix {
            let rowHash = self.hashRow(row: row)
            rowHashes.append(rowHash)
        }

        // Round-robin assign rows to winners
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

        // Mint OutputShares and deposit into winners' collections if linked
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

                let capability = getAccount(addr)
                    .getCapability(/public/HashLayerOutputShareReceiver)
                    .borrow<&{OutputShareReceiver}>()

                if capability != nil {
                    capability!.deposit(share: <- share, shareId: shareId)
                    emit OutputShareMinted(taskId: taskId, shareId: shareId, owner: addr)
                    emit OutputShareDeposited(taskId: taskId, shareId: shareId, owner: addr, depositedToOwner: true)
                } else {
                    // store in fallback contract storage
                    if self.fallbackOutputShares[taskId] == nil {
                        self.fallbackOutputShares[taskId] <- {}
                    }
                    self.fallbackOutputShares[taskId]![shareId] <-! share
                    emit OutputShareMinted(taskId: taskId, shareId: shareId, owner: addr)
                    emit OutputShareDeposited(taskId: taskId, shareId: shareId, owner: addr, depositedToOwner: false)
                }
            }
        }
    }

    // ---------------- Pay to access output ----------------
    // If the share is in fallback storage, contract will route payment to stored owner.
    // If the share has been deposited into a user account, caller must provide the recipient address explicitly.
    pub fun payForOutput(
        taskId: UInt64,
        shareId: UInt64,
        recipient: Address?,
        payer: AuthAccount,
        usageFee: UFix64
    ) {
        var ownerAddr: Address? = nil

        if self.fallbackOutputShares[taskId] != nil {
            let maybeShareRef = &self.fallbackOutputShares[taskId]![shareId] as &OutputShare?
            if maybeShareRef != nil {
                ownerAddr = maybeShareRef!.ownerAddress
            }
        }

        if ownerAddr == nil {
            // require recipient provided for off-contract shares
            if recipient == nil {
                panic("Share not in fallback storage; provide recipient address to pay directly")
            }
            ownerAddr = recipient!
        }

        let payerVault = payer.borrow<&FlowToken.Vault>(from: /storage/flowTokenVault)
            ?? panic("Cannot borrow payer vault")

        let ownerVault = getAccount(ownerAddr!)
            .getCapability(/public/flowTokenReceiver)
            .borrow<&FlowToken.Vault{FungibleToken.Receiver}>()
            ?? panic("Cannot borrow owner vault")

        let payment <- payerVault.withdraw(amount: usageFee)
        ownerVault.deposit(from: <- payment)
        emit OutputPaid(taskId: taskId, shareId: shareId, payer: payer.address, recipient: ownerAddr!, amount: usageFee)
    }

    // ---------------- View helpers ----------------

    // view fallback shares for a task (contract-held shares)
    pub fun getFallbackSharesForTask(taskId: UInt64): [UInt64] {
        if self.fallbackOutputShares[taskId] == nil {
            return []
        }
        return self.fallbackOutputShares[taskId]!.keys
    }

    // allow owner of a fallback share to claim it into their account (must have linked capability)
    pub fun claimFallbackShare(taskId: UInt64, shareId: UInt64, claimer: AuthAccount) {
        let maybeMapRef = &self.fallbackOutputShares[taskId] as @{UInt64: OutputShare}? 
            ?? panic("No fallback shares for task")
        let share <- maybeMapRef.remove(key: shareId) 
            ?? panic("No such fallback share")
        let owner = share.ownerAddress
        if claimer.address != owner {
            // return share back to fallback storage before panic
            self.fallbackOutputShares[taskId]![shareId] <-! share
            panic("Only owner can claim their fallback share")
        }

        let capability = claimer.getCapability(/public/HashLayerOutputShareReceiver)
            .borrow<&{OutputShareReceiver}>()
        if capability == nil {
            // return share back
            self.fallbackOutputShares[taskId]![shareId] <-! share
            panic("Account has not linked an OutputShareReceiver capability")
        }

        capability!.deposit(share: <- share, shareId: shareId)
        emit OutputShareDeposited(taskId: taskId, shareId: shareId, owner: owner, depositedToOwner: true)
    }

    destroy() {
        destroy self.tasks
        // destroy nested fallback maps
        for taskId in self.fallbackOutputShares.keys {
            let mapRef <- self.fallbackOutputShares.remove(key: taskId)!
            destroy mapRef
        }
        destroy self.fallbackOutputShares
    }
}
