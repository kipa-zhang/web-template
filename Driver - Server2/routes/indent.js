const express = require('express');
const router = express.Router();
require('express-async-errors');

router.get('/overview', (req, res) => {
    res.render('indent/overview');
});

router.get('/indentAssigned', (req, res) => {
    res.render('indent/indentAssigned');
});


module.exports = router;
