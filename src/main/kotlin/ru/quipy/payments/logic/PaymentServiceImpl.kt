package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun canAccept(deadline: Long, queuedRequests: Int): Boolean {
        val reserved = ArrayList<PaymentExternalSystemAdapter>()
        for (account in paymentAccounts) {
            if (!account.isEnabled()) continue
            if (!account.canAccept(deadline, queuedRequests)) {
                reserved.forEach { it.releaseAdmission() }
                return false
            }
            reserved.add(account)
        }
        return true
    }

    override fun releaseAdmission() {
        paymentAccounts.filter { it.isEnabled() }.forEach { it.releaseAdmission() }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            if (!account.isEnabled()) continue
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}